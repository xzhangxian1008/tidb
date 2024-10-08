// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package extension

type clearFuncBuilder struct {
	clears []func()
}

// DoWithCollectClear executes a function and collect it clear function
func (b *clearFuncBuilder) DoWithCollectClear(fn func() (func(), error)) error {
	clearFunc, err := fn()
	if err != nil {
		return err
	}

	if clearFunc != nil {
		b.clears = append(b.clears, clearFunc)
	}

	return nil
}

// Build builds a clear function
func (b *clearFuncBuilder) Build() func() {
	return func() {
		for _, fn := range b.clears {
			fn()
		}
	}
}
