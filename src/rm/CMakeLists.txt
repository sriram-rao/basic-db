add_library(rm rm.cc ../utils/copy_utils.h rmindexscan.cc)
add_dependencies(rm rbfm ix googlelog)
target_link_libraries(rm rbfm ix glog)