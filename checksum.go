/*
Copyright 2018 liipx(lipengxiang)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package binlog

import (
	"strconv"
	"strings"
	"unicode"
)

// https://dev.mysql.com/doc/refman/5.6/en/replication-options-binary-log.html#option_mysqld_binlog-checksum
// Beginning with MySQL 5.6.2, MySQL supports reading and writing of binary log checksums.
// ------------------------------------
// | Default Value (>= 5.6.6) | CRC32 |
// | Default Value (<= 5.6.5) | NONE  |
// ------------------------------------
// Enabling this option causes the master to write checksums for events written to the binary log.
// Set to NONE to disable, or the name of the algorithm to be used for generating checksums;
// currently, only CRC32 checksums are supported. As of MySQL 5.6.6, CRC32 is the default.
// This option was added in MySQL 5.6.2.
var mysqlChecksumVersion = (5*256+6)*256 + 2

// this algorithm is base on 'github.com/go-mysql'
func hasChecksum(version string) bool {
	v := [3]int{0, 0, 0}
	checksum := func(v [3]int) int {
		return (v[0]*256+v[1])*256 + v[2]
	}

	defer func() {
		if err := recover(); err != nil {
			return
		}
	}()

	vs := strings.Split(version, ".")
	v[0], _ = strconv.Atoi(vs[0])
	v[1], _ = strconv.Atoi(vs[1])
	if len(vs) < 3 {
		return checksum(v) >= mysqlChecksumVersion
	}

	index := 0
	for i, c := range vs[2] {
		if !unicode.IsNumber(c) {
			index = i
			break
		}
	}

	v[2], _ = strconv.Atoi(vs[2][0:index])
	return checksum(v) >= mysqlChecksumVersion
}