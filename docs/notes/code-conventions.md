<!-- Licensed to the Apache Software Foundation (ASF) under one -->
<!-- or more contributor license agreements.  See the NOTICE file -->
<!-- distributed with this work for additional information -->
<!-- regarding copyright ownership.  The ASF licenses this file -->
<!-- to you under the Apache License, Version 2.0 (the -->
<!-- "License"); you may not use this file except in compliance -->
<!-- with the License.  You may obtain a copy of the License at -->

<!--   http://www.apache.org/licenses/LICENSE-2.0 -->

<!-- Unless required by applicable law or agreed to in writing, -->
<!-- software distributed under the License is distributed on an -->
<!-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY -->
<!-- KIND, either express or implied.  See the License for the -->
<!-- specific language governing permissions and limitations -->
<!-- under the License. -->

# Code conventions

## C conventions

Use `clang-format` to apply the styles configured in the `.clang-format` file.

    $ clang-format -i <your-file>

Use the [`git-clang-format`](https://github.com/llvm/llvm-project/blob/main/clang/tools/clang-format/git-clang-format)
script to only reformat your outstanding modified files in a git repository.

Reformat entire repository using the following command, then analyze how much has changed

    $ clang-format -i $(git ls-files -- '*.cpp' '*.hpp' '*.c' '*.h')
    $ git diff -w --shortstat

To disable automatic formatting in a particular part of your file,
bracket it with `clang-format on` and `off` commands:

    // clang-format off
    static void* do();
    // clang-format on

The options available in `.clang-format` file are explained in
<https://clang.llvm.org/docs/ClangFormatStyleOptions.html> documentation page.

See <https://clang.llvm.org/docs/ClangFormat.html> more information
about Clang formatting.

## Python conventions

Python code should be http://www.python.org/dev/peps/pep-0008/[PEP-8]
compliant. In particular:

* Use four-space indents
* Do not use studlyCaps for function, method, and variable names.
  Instead use `underscore_separated_names`.
