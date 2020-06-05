load("@rules_cc//cc:defs.bzl", "cc_binary")

cc_test(
    name = "theta_test",
    srcs = glob(["*.cc"]),
    copts = [
        "-Ithird_party/incubator-datasketches-cpp/theta",
        "-Ithird_party/incubator-datasketches-cpp/common",
        "-Itheta_dup/include",
    ],
    deps = [
        "//third_party/incubator-datasketches-cpp:theta",
        "//theta_dup:theta_dup",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
    ],
)
