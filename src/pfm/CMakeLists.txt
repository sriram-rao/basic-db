add_library(pfm pfm.cc ../rbfm/record.cc ../include/record.h ../rbfm/slotdir.cc ../include/slotdir.h)
add_dependencies(pfm googlelog)
target_link_libraries(pfm glog)