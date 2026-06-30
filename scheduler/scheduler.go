/*
 *   Copyright (c) 2024 Arcology Network

 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.

 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.

 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package scheduler

import (
	"runtime"
	"sort"

	queue "github.com/arcology-network/common-lib/exp/queue"
	"github.com/arcology-network/common-lib/exp/slice"
	libtypes "github.com/arcology-network/common-lib/types"
	callee "github.com/arcology-network/scheduler/callee"
	profile "github.com/arcology-network/scheduler/callee"
	"github.com/arcology-network/scheduler/conflictor"
	workload "github.com/arcology-network/scheduler/workload"
	ethcommon "github.com/ethereum/go-ethereum/common"
)

type Scheduler struct {
	*profile.ProfileStore
	latest *workload.ExecutionPlan

	// If the scheduler should skip planning deferred executions. If true, the scheduler will not plan deferred executions by default.
	SkipDeferred bool
}

// Initialize a new scheduler, the fileName is the file path to the scheduler's conflict database and the deferByDefault
// instructs the scheduler to schedule the deferred transactions if it is true.
func NewScheduler(manager *profile.ProfileStore) (*Scheduler, error) {
	return &Scheduler{
		ProfileStore: manager,
	}, nil
}

// For debugging purpose only.
func (this *Scheduler) GetLatest() *workload.ExecutionPlan    { return this.latest }
func (this *Scheduler) SetLatest(sch *workload.ExecutionPlan) { this.latest = sch }
func (this *Scheduler) HasLatest() bool                       { return this.latest != nil }
func (this *Scheduler) ClearLatest()                          { this.latest = nil }

// The scheduler will optimize the given transactions and return a schedule.
// The schedule will contain the transactions that can be executed in parallel and the ones that have to be executed sequentially.
func (this *Scheduler) New(stdMsgs []*libtypes.StandardMessage) (*workload.ExecutionPlan, error) {
	// Get the static schedule for the given transactions first.
	// sch, pendingJobs := this.StaticSchedule(stdMsgs) // The pendingJobs are the transactions that need to be scheduled to avoid conflicts.

	this.latest = &workload.ExecutionPlan{
		Store: this.ProfileStore.StateStore(),
	}

	if len(stdMsgs) == 0 {
		return this.latest, nil // No transactions to process.
	}

	// Create conflict lookup for each job.
	jobs := this.CreateJobs(stdMsgs)
	for _, job := range jobs {
		job.GenerateConflictLookup()
	}

	// Group the jobs into queues by their sender addresses.
	msgQueuesBySenders := this.QueueBySender(jobs)

	// The code below will search for the parallel transaction set from a set of conflicting transactions.
	// Whataever left is the sequential transaction set after this.
	for {
		// Always start from the queue with the least number of conflicts.
		// We need to resort the queues here because the number of conflicts may change
		// as we dequeue transactions from the queues.
		sort.Slice(msgQueuesBySenders, func(i, j int) bool {
			lft, _ := msgQueuesBySenders[i].Peek()
			rgt, _ := msgQueuesBySenders[j].Peek()
			if lft.NumConflicts() != rgt.NumConflicts() {
				return lft.NumConflicts() < rgt.NumConflicts()
			}
			return lft.StdMsg.ID < rgt.StdMsg.ID
		})

		seedJob, _ := msgQueuesBySenders[0].Dequeue()
		paraJobSet := []*workload.Job{seedJob} // A set of conflict free transactions, starting with the first profile.

		//Sequential only profiles to the sequential execution set.
		if seedJob.IsSequentialOnly() {
			// Do nothing
		} else {
			// This set containes all the conflicts of the current parallel transaction set.
			// Any transaction that conflicts with any of them cannot be added to the parallel set.
			// Look for the parallel transactions that aren't conflicting with the current set of transactions.
			for {
				length := len(paraJobSet)
				for i := range msgQueuesBySenders {
					if msgQueuesBySenders[i].IsEmpty() {
						continue // Already processed.
					}

					// We aren't sure if there is a conflict yet, peek first.
					job, _ := msgQueuesBySenders[i].Peek()
					if job.IsFullyParallelizable() || job.IsPotentiallyParallelizable() {
						paraJobSet = append(paraJobSet, job)
						msgQueuesBySenders[i].Dequeue() // Remove from the pending queue.
						continue
					}

					// The current profile isn't conflicting with any transaction in the unique profile set
					// so it can be added to the parallel transaction set. Because the conflict info is always symmetric,
					// we only need to check one way.
					if !workload.Jobs(paraJobSet).HasConflictWith(job) {
						paraJobSet = append(paraJobSet, job)
						msgQueuesBySenders[i].Dequeue() // Remove from the pending queue.
					}
				}

				if len(paraJobSet) == length {
					break // No more jobs can be added.
				}
			}
		}

		// Only one transaction in the parallel set, no need to proceed with planning deferred execution.
		if len(paraJobSet) == 1 && !this.SkipDeferred {
			if paraJobSet[0].Profile.IsDeferrable() {
				paraJobSet[0].IsDeferred = true
			}

			jobSeq := &workload.JobSequence{
				Jobs: paraJobSet,
			}

			this.latest.Generations = append(this.latest.Generations,
				workload.NewGeneration(uint32(runtime.NumCPU()), []*workload.JobSequence{jobSeq}))

		} else {
			// Look for the deferred transactions and add them to the deferred transaction set.
			paraSeqs, deferredSeqs := this.ScheduleWithDeferred(paraJobSet)

			// Insert the parallel transaction first
			if len(paraSeqs) > 0 {
				// Sort by the first job's message ID to ensure consistent ordering.
				// This won't affect the execution results, as the sequences in the
				// same generation are executed in parallel anyway.
				workload.SortJobSequences(paraSeqs)

				this.latest.Generations = append(this.latest.Generations,
					workload.NewGeneration(uint32(runtime.NumCPU()), paraSeqs))
			}

			// Insert the deferred transaction generation.
			if len(deferredSeqs) > 0 {
				workload.SortJobSequences(deferredSeqs)
				this.latest.Generations = append(this.latest.Generations,
					workload.NewGeneration(uint32(runtime.NumCPU()), deferredSeqs),
				)
			}
		}

		slice.RemoveIf(
			&msgQueuesBySenders,
			func(_ int, msgQ *queue.Queue[*workload.Job]) bool {
				return msgQ.IsEmpty()
			}) // Remove empty queues.

		if len(msgQueuesBySenders) == 0 {
			break // Nothing left to process.
		}
	}

	return this.latest, this.latest.Finalize()
}

// The scheduler will scan through and look for multipl instances of the same profile and put one of them in the second
// consecutive set of transactions for deferred execution.
func (this *Scheduler) ScheduleWithDeferred(paraJobSet []*workload.Job) ([]*workload.JobSequence, []*workload.JobSequence) {
	// Group by profile UID.
	_, jobSets := slice.GroupBy(paraJobSet, func(_ int, pair *workload.Job) *uint64 {
		if pair.Profile == nil {
			return nil
		}
		return &pair.Profile.ID.UID
	})

	paraGen := []*workload.JobSequence{}     // The parallel transaction generation.
	deferredGen := []*workload.JobSequence{} // The deferred transaction generation.
	for _, jobs := range jobSets {
		paraJobs := slice.Transform(jobs,
			func(i int, job *workload.Job) *workload.JobSequence {
				return &workload.JobSequence{
					ID:   uint64(i),
					Jobs: []*workload.Job{job},
				}
			},
		)
		//  IsDeferrable == false both
		// There is more than one transaction for the same profile and the first one is marked as deferrable.
		if len(jobs) > 1 && jobs[0].Profile.IsDeferrable() {
			def := *slice.PopBack(&paraJobs) // Use the last one as the deferred transaction.

			// Mark the job as deferred.
			slice.Foreach(def.Jobs, func(_ int, job **workload.Job) {
				(*job).IsDeferred = true
			})

			if len(jobs) == 1 {
				// Only one transaction for the profile, no need to add to the deferred generation. Add it back to the parallel generation.
				paraJobs = append(paraJobs, def)
				continue
			}

			// Add the deferred transaction to the new generation.
			deferredGen = append(deferredGen, def)
		}
		paraGen = append(paraGen, paraJobs...)
	}
	return paraGen, deferredGen
}

// CreateJobs creates jobs for the given standard messages.
func (this *Scheduler) CreateJobs(stdMsgs []*libtypes.StandardMessage) []*workload.Job {
	return slice.ParallelTransform(
		stdMsgs,
		8,
		func(i int, msg *libtypes.StandardMessage) *workload.Job {
			addr, selector := msg.GetAddressAndSelector()
			profile, _ := this.ProfileStore.LoadIfExists(msg.ID, addr, selector)
			if profile == nil {
				profile = callee.NewProfile(msg.ID, addr, selector, this.ProfileStore)
			}

			return &workload.Job{
				StdMsg:  msg,
				Profile: profile,
			}
		})
}

// Convert the standard messages to message queues base on their sender addresses for scheduling.
func (this *Scheduler) QueueBySender(jobs []*workload.Job) []*queue.Queue[*workload.Job] {
	// jobs := this.CreateJobs(stdMsgs)
	// Sort the profiles by the number of conflicts and the profile index in ascending order.
	sort.SliceStable(jobs, func(i, j int) bool {
		if lft, rgt := jobs[i].NumConflicts(), jobs[j].NumConflicts(); lft != rgt {
			return lft < rgt
		}
		return jobs[i].StdMsg.ID < jobs[j].StdMsg.ID
	})

	_, msgSet := slice.GroupBy(jobs,
		func(_ int, job *workload.Job) *ethcommon.Address {
			return &job.StdMsg.Native.From
		})

	// Create queues for each sender address.
	// Within each queue, the jobs are sorted by their nonces in ascending order.
	// So that they can be processed in the correct order.
	bySender := make([]*queue.Queue[*workload.Job], len(msgSet))
	for i, msgs := range msgSet {
		bySender[i] = queue.NewSortedQueueFromSlice(msgs, func(lft, rgt *workload.Job) bool {
			return lft.StdMsg.Native.Nonce < rgt.StdMsg.Native.Nonce
		})
	}
	return bySender
}

// ImportCollisions imports the collision information detected by the conflictor
// into the scheduler's profile store, mapping conflicts to the original callee profiles.
func (this *Scheduler) ImportCollisions(conflictSet *conflictor.CollisionSummary) error {
	// Map the conflict info to the original callee profiles using UID as the key.
	for _, conflictInfo := range conflictSet.Collisions {
		// Map back to their orginal callee profile IDs
		selfID, peerIDs := conflictInfo.MapConflictToCallee(this.latest.JobIDLookup)

		// Get the profiles by IDs and add the conflict peers.
		selfProfile, err := this.ProfileStore.LoadOrCreate(selfID)
		if err != nil {
			return err
		}

		for _, peerID := range peerIDs {
			peerProfile, err := this.ProfileStore.LoadOrCreate(peerID)
			if err != nil {
				return err
			}
			peerProfile.CrossLink(selfProfile) // Add each other as conflict peers.
		}
	}
	return nil
}
