add_library(rbfm rbfm.cc page.cc record.cc slotdirectory.cc rbfmscanner.cc)
add_dependencies(rbfm pfm googlelog)
target_link_libraries(rbfm pfm glog)