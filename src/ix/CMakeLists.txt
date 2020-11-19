add_library(ix ix.cc ixfilehandle.cc node.cc ../utils/parse_utils.h ixscanner.cc ../utils/compare_utils.h)
add_dependencies(ix pfm googlelog)
target_link_libraries(ix pfm glog)