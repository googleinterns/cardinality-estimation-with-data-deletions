load("@rules_cc//cc:defs.bzl", "cc_binary")

cc_binary(
    name = "test",
    srcs = ["test.cc"],
    copts = [
        "-Ithird_party/incubator-datasketches-cpp/theta",
        "-Ithird_party/incubator-datasketches-cpp/common",
    ],
    deps = [
        "//third_party/incubator-datasketches-cpp:theta",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
    ],
)
