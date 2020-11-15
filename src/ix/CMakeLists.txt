add_library(ix ix.cc ixfilehandle.cc)
add_dependencies(ix pfm googlelog)
target_link_libraries(ix pfm glog)