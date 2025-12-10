import {
  jest,
  describe,
  test,
  expect,
  beforeEach,
  afterEach,
} from "@jest/globals";
import { createBatcher } from "../src/batcher.js";

describe("createBatcher", () => {
  let mockExternalService;
  let consoleSpy;

  beforeEach(() => {
    mockExternalService = {
      call: jest.fn(),
    };
    consoleSpy = jest.spyOn(console, "log").mockImplementation(() => {});
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  describe("Basic Functionality", () => {
    test("should batch products and send on flush", () => {
      const batcher = createBatcher(5, mockExternalService);
      const products = [
        { id: "1", title: "Product 1", description: "Description 1" },
        { id: "2", title: "Product 2", description: "Description 2" },
      ];

      batcher.addProducts(products);
      batcher.flush();

      expect(mockExternalService.call).toHaveBeenCalledTimes(1);
      const sentBatch = JSON.parse(mockExternalService.call.mock.calls[0][0]);
      expect(sentBatch).toEqual(products);
    });

    test("should not send empty batch", () => {
      const batcher = createBatcher(5, mockExternalService);
      batcher.flush();
      expect(mockExternalService.call).not.toHaveBeenCalled();
    });
  });

  describe("Size Limit Enforcement", () => {
    test("should split into multiple batches when exceeding size limit", () => {
      const batcher = createBatcher(0.001, mockExternalService); // 1KB limit
      const products = Array.from({ length: 20 }, (_, i) => ({
        id: `${i}`,
        title: `Product ${i}`,
        description: "A".repeat(100),
      }));

      batcher.addProducts(products);
      batcher.flush();

      expect(mockExternalService.call.mock.calls.length).toBeGreaterThan(1);

      // Verify all products were sent
      let totalProducts = 0;
      mockExternalService.call.mock.calls.forEach((call) => {
        const batch = JSON.parse(call[0]);
        totalProducts += batch.length;
      });
      expect(totalProducts).toBe(20);
    });

    test("should never exceed max batch size", () => {
      const maxSizeMB = 0.01; // 10KB
      const maxSizeBytes = maxSizeMB * 1_048_576;
      const batcher = createBatcher(maxSizeMB, mockExternalService);

      const products = Array.from({ length: 100 }, (_, i) => ({
        id: `${i}`,
        title: `Product ${i}`,
        description: "Test description with some content",
      }));

      batcher.addProducts(products);
      batcher.flush();

      mockExternalService.call.mock.calls.forEach((call) => {
        const batchSize = Buffer.byteLength(call[0], "utf8");
        expect(batchSize).toBeLessThanOrEqual(maxSizeBytes);
      });
    });
  });

  describe("Data Integrity", () => {
    test("should maintain product order across batches", () => {
      const batcher = createBatcher(0.01, mockExternalService);
      const products = Array.from({ length: 50 }, (_, i) => ({
        id: `${i}`,
        title: `Product ${i}`,
        description: "Test",
      }));

      batcher.addProducts(products);
      batcher.flush();

      const allSentProducts = [];
      mockExternalService.call.mock.calls.forEach((call) => {
        const batch = JSON.parse(call[0]);
        allSentProducts.push(...batch);
      });

      allSentProducts.forEach((product, index) => {
        expect(product.id).toBe(`${index}`);
      });
    });
  });
});
