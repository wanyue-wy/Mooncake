#include <gtest/gtest.h>
#include "tiered_cache/copier_registry.h"
#include "tiered_cache/data_copier.h"

namespace mooncake {

// ============================================================
// CopierRegistry tests
// ============================================================

class CopierRegistryTest : public ::testing::Test {
   protected:
    // Helper: a trivial copier that just returns OK
    static tl::expected<void, ErrorCode> DummyCopier(const DataSource& src,
                                                     const DataSource& dst) {
        return {};
    }
};

TEST_F(CopierRegistryTest, SingletonReturnsSameInstance) {
    auto& inst1 = CopierRegistry::GetInstance();
    auto& inst2 = CopierRegistry::GetInstance();
    EXPECT_EQ(&inst1, &inst2);
}

TEST_F(CopierRegistryTest, RegisterMemoryTypeAddsEntry) {
    auto& reg = CopierRegistry::GetInstance();
    size_t before = reg.GetMemoryTypeRegistrations().size();

    // Register a custom memory type (use ASCEND_NPU to avoid collisions)
    reg.RegisterMemoryType(MemoryType::ASCEND_NPU, DummyCopier, DummyCopier);

    size_t after = reg.GetMemoryTypeRegistrations().size();
    EXPECT_EQ(after, before + 1);
}

TEST_F(CopierRegistryTest, RegisterDirectPathAddsEntry) {
    auto& reg = CopierRegistry::GetInstance();
    size_t before = reg.GetDirectPathRegistrations().size();

    reg.RegisterDirectPath(MemoryType::DRAM, MemoryType::ASCEND_NPU,
                           DummyCopier);

    size_t after = reg.GetDirectPathRegistrations().size();
    EXPECT_EQ(after, before + 1);
}

// ============================================================
// DataCopierBuilder tests
// ============================================================

class DataCopierBuilderTest : public ::testing::Test {
   protected:
    // A simple copy function that copies nothing but succeeds
    static tl::expected<void, ErrorCode> SuccessCopier(const DataSource& src,
                                                       const DataSource& dst) {
        return {};
    }

    // A copy function that always fails
    static tl::expected<void, ErrorCode> FailCopier(const DataSource& src,
                                                    const DataSource& dst) {
        return tl::unexpected(ErrorCode::DATA_COPY_FAILED);
    }
};

TEST_F(DataCopierBuilderTest, BuildSucceeds) {
    // Builder should succeed because DRAM copier is already registered
    DataCopierBuilder builder;
    auto copier = builder.Build();
    EXPECT_NE(copier, nullptr);
}

TEST_F(DataCopierBuilderTest, AddDirectPathAndBuild) {
    DataCopierBuilder builder;

    // Add a custom direct path (DRAM -> DRAM)
    builder.AddDirectPath(MemoryType::DRAM, MemoryType::DRAM, SuccessCopier);

    auto copier = builder.Build();
    EXPECT_NE(copier, nullptr);
}

TEST_F(DataCopierBuilderTest, ChainingWorks) {
    DataCopierBuilder builder;

    auto& ref = builder.AddDirectPath(MemoryType::DRAM, MemoryType::DRAM,
                                      SuccessCopier);
    // Chaining should return the same builder
    EXPECT_EQ(&ref, &builder);
}

// ============================================================
// DataCopier Copy tests
// ============================================================

class DataCopierTest : public ::testing::Test {
   protected:
    std::unique_ptr<DataCopier> copier_;

    void SetUp() override {
        DataCopierBuilder builder;
        copier_ = builder.Build();
    }
};

TEST_F(DataCopierTest, CopyDRAMToDRAM) {
    // Prepare simple DRAM DataSources
    const size_t data_size = 256;
    auto src_buf = std::make_unique<char[]>(data_size);
    std::memset(src_buf.get(), 0xAB, data_size);

    auto dst_buf = std::make_unique<char[]>(data_size);
    std::memset(dst_buf.get(), 0, data_size);

    DataSource src{
        std::make_unique<TempDRAMBuffer>(std::move(src_buf), data_size),
        MemoryType::DRAM};
    DataSource dst{
        std::make_unique<TempDRAMBuffer>(std::move(dst_buf), data_size),
        MemoryType::DRAM};

    auto result = copier_->Copy(src, dst);
    EXPECT_TRUE(result.has_value());
}

}  // namespace mooncake
