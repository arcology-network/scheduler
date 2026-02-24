/*
 *   Copyright (c) 2026 Arcology Network

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

package common

import "errors"

var WARN_OUT_OF_LOWER_LIMIT = errors.New("Warning: Out of the lower limit!")
var WARN_OUT_OF_UPPER_LIMIT = errors.New("Warning: Out of the upper limit!")

var WARN_ACCESS_CONFLICT = errors.New("Warning: State access conflict detected!")
var WARN_UPSTREAM_CONFLICT_IN_SEQUENCE = errors.New("Warning: Upstream transaction conflicted. Invalidate the execution chain!")
