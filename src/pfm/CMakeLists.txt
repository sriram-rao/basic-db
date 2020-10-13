add_library(pfm pfm.cc ../rbfm/record.cc ../rbfm/page.cc ../include/page.h)
add_dependencies(pfm googlelog)
target_link_libraries(pfm glog)