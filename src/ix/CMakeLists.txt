add_library(ix ix.cc ixfilehandle.cc node.cc ../utils/parse_utils.h ixscanner.cc)
add_dependencies(ix pfm googlelog)
target_link_libraries(ix pfm glog)