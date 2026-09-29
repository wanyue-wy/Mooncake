/**
 * Architecture-independent IPv6 address parsing tests for Mooncake Store.
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "common.h"

namespace mooncake {
namespace testing {

//=============================================================================
// Unit tests for IPv6 address parsing functions
//=============================================================================

class IPv6ParsingTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("IPv6ParsingTest");
        FLAGS_logtostderr = 1;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }
};

// Test isValidIpV6 function with various IPv6 address formats
TEST_F(IPv6ParsingTest, IsValidIpV6) {
    // Valid IPv6 addresses
    EXPECT_TRUE(isValidIpV6("::1")) << "Loopback address should be valid";
    EXPECT_TRUE(isValidIpV6("::")) << "Any address should be valid";
    EXPECT_TRUE(isValidIpV6("2001:db8::1")) << "Global unicast should be valid";
    EXPECT_TRUE(isValidIpV6("fe80::1"))
        << "Link-local without scope should be valid";
    EXPECT_TRUE(isValidIpV6("fe80::a236:bcff:fecb:a1be"))
        << "Full link-local should be valid";

    // Valid IPv6 addresses with scope ID
    EXPECT_TRUE(isValidIpV6("fe80::1%eth0"))
        << "Link-local with scope ID should be valid";
    EXPECT_TRUE(isValidIpV6("fe80::a236:bcff:fecb:a1be%eno2"))
        << "Full link-local with scope ID should be valid";

    // Invalid: IPv6 with port (should not be considered valid pure IPv6)
    EXPECT_FALSE(isValidIpV6("fe80::1%eth0:12345"))
        << "IPv6 with scope ID and port should be invalid";
    EXPECT_FALSE(isValidIpV6("fe80::a236:bcff:fecb:a1be%eno2:17813"))
        << "Full address with scope and port should be invalid";

    // Invalid addresses
    EXPECT_FALSE(isValidIpV6("192.168.1.1")) << "IPv4 should be invalid";
    EXPECT_FALSE(isValidIpV6("localhost")) << "Hostname should be invalid";
    EXPECT_FALSE(isValidIpV6("")) << "Empty string should be invalid";
    EXPECT_FALSE(isValidIpV6("not-an-ip")) << "Random string should be invalid";
}

// Test parseHostNameWithPort function with IPv6 addresses
TEST_F(IPv6ParsingTest, ParseHostNameWithPort) {
    // Test bracketed IPv6 with port
    {
        auto [host, port] = parseHostNameWithPort("[::1]:17813");
        EXPECT_EQ(host, "::1") << "Should extract loopback address";
        EXPECT_EQ(port, 17813) << "Should extract port 17813";
    }

    // Test bracketed link-local with scope ID and port
    {
        auto [host, port] =
            parseHostNameWithPort("[fe80::a236:bcff:fecb:a1be%eno2]:17813");
        EXPECT_EQ(host, "fe80::a236:bcff:fecb:a1be%eno2")
            << "Should preserve scope ID";
        EXPECT_EQ(port, 17813) << "Should extract port 17813";
    }

    // Test unbracketed IPv6 with scope ID and port (common in internal usage)
    {
        auto [host, port] =
            parseHostNameWithPort("fe80::a236:bcff:fecb:a1be%eno2:15773");
        EXPECT_EQ(host, "fe80::a236:bcff:fecb:a1be%eno2")
            << "Should correctly parse host with scope ID";
        EXPECT_EQ(port, 15773) << "Should extract correct port";
    }

    // Test pure IPv6 without port (should use default handshake port)
    {
        auto [host, port] = parseHostNameWithPort("::1");
        EXPECT_EQ(host, "::1") << "Should return loopback address";
        EXPECT_EQ(port, getDefaultHandshakePort())
            << "Should use default handshake port";
    }

    // Test link-local with scope ID but no port
    {
        auto [host, port] =
            parseHostNameWithPort("fe80::a236:bcff:fecb:a1be%eno2");
        EXPECT_EQ(host, "fe80::a236:bcff:fecb:a1be%eno2")
            << "Should preserve full address with scope";
        EXPECT_EQ(port, getDefaultHandshakePort())
            << "Should use default handshake port";
    }

    // Test IPv4 address (should still work)
    {
        auto [host, port] = parseHostNameWithPort("192.168.1.1:8080");
        EXPECT_EQ(host, "192.168.1.1") << "Should extract IPv4 address";
        EXPECT_EQ(port, 8080) << "Should extract port";
    }

    // Test hostname with port
    {
        auto [host, port] = parseHostNameWithPort("localhost:17813");
        EXPECT_EQ(host, "localhost") << "Should extract hostname";
        EXPECT_EQ(port, 17813) << "Should extract port";
    }
}

// Test maybeWrapIpV6 function
TEST_F(IPv6ParsingTest, MaybeWrapIpV6) {
    // IPv6 addresses should be wrapped
    EXPECT_EQ(maybeWrapIpV6("::1"), "[::1]") << "Loopback should be wrapped";
    EXPECT_EQ(maybeWrapIpV6("fe80::1%eth0"), "[fe80::1%eth0]")
        << "Link-local with scope should be wrapped";
    EXPECT_EQ(maybeWrapIpV6("fe80::a236:bcff:fecb:a1be%eno2"),
              "[fe80::a236:bcff:fecb:a1be%eno2]")
        << "Full link-local should be wrapped";

    // Non-IPv6 should not be wrapped
    EXPECT_EQ(maybeWrapIpV6("192.168.1.1"), "192.168.1.1")
        << "IPv4 should not be wrapped";
    EXPECT_EQ(maybeWrapIpV6("localhost"), "localhost")
        << "Hostname should not be wrapped";
}

// Test that IPv6 address with different formats are handled correctly
TEST_F(IPv6ParsingTest, IPv6AddressFormatVariations) {
    // Test different IPv6 address formats that should be parsed correctly
    std::vector<std::pair<std::string, std::pair<std::string, uint16_t>>>
        test_cases = {
            {"[::1]:8080", {"::1", 8080}},
            {"[2001:db8::1]:9000", {"2001:db8::1", 9000}},
            {"[fe80::1%lo]:7000", {"fe80::1%lo", 7000}},
        };

    for (const auto& [input, expected] : test_cases) {
        auto [host, port] = parseHostNameWithPort(input);
        EXPECT_EQ(host, expected.first) << "Host mismatch for input: " << input;
        EXPECT_EQ(port, expected.second)
            << "Port mismatch for input: " << input;
    }
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    return RUN_ALL_TESTS();
}
