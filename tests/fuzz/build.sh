#!/bin/bash -eu
# Copyright 2018 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
################################################################################
mkdir build
pushd build
cmake .. -DCMAKE_BUILD_TYPE=Debug -DENABLE_FUZZ_TESTING=ON -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=OFF -DFUZZ_REGRESSION_TESTS=OFF -DCMAKE_C_FLAGS=-DQD_MEMORY_DEBUG -DRUNTIME_CHECK=asan
VERBOSE=1 make
cp tests/fuzz/fuzz_http2_decoder $OUT/
popd
zip -j $OUT/fuzz_http2_decoder_seed_corpus.zip tests/fuzz/fuzz_http2_decoder/corpus/* tests/fuzz/fuzz_http2_decoder/crash/*
