add_library(rm rm.cc ../utils/copy_utils.h rmindexscan.cc rmindexscan.h)
add_dependencies(rm rbfm ix googlelog)
target_link_libraries(rm rbfm ix glog)