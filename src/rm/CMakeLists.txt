add_library(rm rm.cc ../utils/copy_utils.h)
add_dependencies(rm rbfm googlelog)
target_link_libraries(rm rbfm glog)