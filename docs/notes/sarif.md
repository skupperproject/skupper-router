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

# SARIF (Static Analysis Results Interchange Format)

[Introduction from Microsoft](https://github.com/microsoft/sarif-tutorials).

SARIF is a format based on JSON, used to capture warning messages from software tools that work with sourcecode.
Most importantly it can hold compiler warnings and findings from security scanners.
GitHub can then import these and maintain a browsable database of outstanding ones.

## Helpful tooling

- [SARIF validator](https://sarifweb.azurewebsites.net/Validation)
- [SARIF multitool for merging and manipulating .sarif files](https://github.com/microsoft/sarif-sdk/blob/main/docs/multitool-usage.md)
  - [GitHub action to invoke the tool](https://github.com/marketplace/actions/sarif-multitool)
- [GitHub documentation about importing SARIF results](https://docs.github.com/en/code-security/code-scanning/integrating-with-code-scanning/uploading-a-sarif-file-to-github)
