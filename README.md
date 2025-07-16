# BasicDB

#### This is the project codebase for BasicDB


### Components:
 - Paged File Manager
 - Record Based File Manager
 - Relation Manager
 - Index Manager
 - Query Engine

### Build
 - From the repo root directory, create and go into a build directory
 
 `mkdir -p cmake-build-debug && cd cmake-build-debug`

 - Generate makefiles with `cmake` in the build directory by specifying the project root directory as the source:
 
 `cmake ../` 
 
  your makefiles should be written to `[root]/cmake-build-debug`

 - Build the project in the build directory:
 
 `cmake --build .`

 - To run tests with `ctest` in the build directory:
 
 `ctest .`
 
  or you can specify a test case, for example `ctest . -R PFM_File_Test.create_file`
 
 - To clean the build, in the build directory:
 
 `make clean`
 
 or simply remove the build directory:
 `rm -rf [root]/cmake-build-debug`
 
# Or, use CLion
