add_library(pfm pfm.cc ../rbfm/record.cc ../rbfm/page.cc)
add_dependencies(pfm googlelog)
target_link_libraries(pfm glog)