import {
  jest,
  describe,
  test,
  expect,
  beforeEach,
  afterEach,
} from "@jest/globals";
import { parseXMLFeed } from "../src/parser.js";
import { createBatcher } from "../src/batcher.js";
import ExternalService from "../src/external-service.js";

describe("Integration Tests", () => {
  let consoleSpy;

  beforeEach(() => {
    consoleSpy = jest.spyOn(console, "log").mockImplementation(() => {});
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  test("should parse XML and batch products correctly", () => {
    const xmlContent = `<?xml version='1.0' encoding='utf-8'?>
<rss version='2.0' xmlns:g="http://base.google.com/ns/1.0">
  <channel>
    <item>
      <g:id>1</g:id>
      <title>Product 1</title>
      <description>Description 1</description>
    </item>
    <item>
      <g:id>2</g:id>
      <title>Product 2</title>
      <description>Description 2</description>
    </item>
  </channel>
</rss>`;

    const products = parseXMLFeed(xmlContent);
    const mockService = { call: jest.fn() };
    const batcher = createBatcher(5, mockService);

    batcher.addProducts(products);
    batcher.flush();

    expect(mockService.call).toHaveBeenCalledTimes(1);
    const sentBatch = JSON.parse(mockService.call.mock.calls[0][0]);
    expect(sentBatch).toHaveLength(2);
    expect(sentBatch[0].id).toBe("1");
    expect(sentBatch[1].id).toBe("2");
  });

  test("should split large datasets into multiple batches", () => {
    const items = Array.from(
      { length: 100 },
      (_, i) => `
    <item>
      <g:id>${i + 1}</g:id>
      <title>Product ${i + 1}</title>
      <description>${"A".repeat(200)}</description>
    </item>`
    ).join("");

    const xmlContent = `<?xml version='1.0' encoding='utf-8'?>
<rss version='2.0' xmlns:g="http://base.google.com/ns/1.0">
  <channel>${items}
  </channel>
</rss>`;

    const products = parseXMLFeed(xmlContent);
    const mockService = { call: jest.fn() };
    const batcher = createBatcher(0.01, mockService); // 10KB batches

    batcher.addProducts(products);
    batcher.flush();

    expect(products).toHaveLength(100);
    expect(mockService.call.mock.calls.length).toBeGreaterThan(1);

    let totalProducts = 0;
    mockService.call.mock.calls.forEach((call) => {
      const batch = JSON.parse(call[0]);
      totalProducts += batch.length;
    });
    expect(totalProducts).toBe(100);
  });

  test("should respect 5MB batch size limit", () => {
    const items = Array.from(
      { length: 500 },
      (_, i) => `
    <item>
      <g:id>${i}</g:id>
      <title>Product ${i}</title>
      <description>${"A".repeat(500)}</description>
    </item>`
    ).join("");

    const xmlContent = `<?xml version='1.0' encoding='utf-8'?>
<rss version='2.0' xmlns:g="http://base.google.com/ns/1.0">
  <channel>${items}
  </channel>
</rss>`;

    const products = parseXMLFeed(xmlContent);
    const mockService = { call: jest.fn() };
    const maxSizeMB = 5;
    const maxSizeBytes = maxSizeMB * 1_048_576;
    const batcher = createBatcher(maxSizeMB, mockService);

    batcher.addProducts(products);
    batcher.flush();

    mockService.call.mock.calls.forEach((call) => {
      const batchSize = Buffer.byteLength(call[0], "utf8");
      expect(batchSize).toBeLessThanOrEqual(maxSizeBytes);
    });
  });

  test("should handle complete workflow with real external service", () => {
    const xmlContent = `<?xml version='1.0' encoding='utf-8'?>
<rss version='2.0' xmlns:g="http://base.google.com/ns/1.0">
  <channel>
    <item>
      <g:id>1</g:id>
      <title>Product 1</title>
      <description>Description 1</description>
    </item>
  </channel>
</rss>`;

    const products = parseXMLFeed(xmlContent);
    const externalService = ExternalService();
    const batcher = createBatcher(5, externalService);

    expect(() => {
      batcher.addProducts(products);
      batcher.flush();
    }).not.toThrow();

    expect(products).toHaveLength(1);
  });

  test("should handle products with missing fields", () => {
    const xmlContent = `<?xml version='1.0' encoding='utf-8'?>
<rss version='2.0' xmlns:g="http://base.google.com/ns/1.0">
  <channel>
    <item>
      <g:id>1</g:id>
      <title>Product with all fields</title>
      <description>Full description</description>
    </item>
    <item>
      <g:id>2</g:id>
      <title>Product without description</title>
    </item>
  </channel>
</rss>`;

    const products = parseXMLFeed(xmlContent);
    const mockService = { call: jest.fn() };
    const batcher = createBatcher(5, mockService);

    batcher.addProducts(products);
    batcher.flush();

    const sentBatch = JSON.parse(mockService.call.mock.calls[0][0]);
    expect(sentBatch).toHaveLength(2);
    expect(sentBatch[1].description).toBe("");
  });

  test("should process 1000 products efficiently", () => {
    const items = Array.from(
      { length: 1000 },
      (_, i) => `
    <item>
      <g:id>${i + 1}</g:id>
      <title>Product ${i + 1}</title>
      <description>Description for product ${i + 1}</description>
    </item>`
    ).join("");

    const xmlContent = `<?xml version='1.0' encoding='utf-8'?>
<rss version='2.0' xmlns:g="http://base.google.com/ns/1.0">
  <channel>${items}
  </channel>
</rss>`;

    const startTime = Date.now();
    const products = parseXMLFeed(xmlContent);
    const mockService = { call: jest.fn() };
    const batcher = createBatcher(0.1, mockService);
    batcher.addProducts(products);
    batcher.flush();
    const endTime = Date.now();

    expect(products).toHaveLength(1000);
    expect(endTime - startTime).toBeLessThan(2000);

    let totalProducts = 0;
    mockService.call.mock.calls.forEach((call) => {
      const batch = JSON.parse(call[0]);
      totalProducts += batch.length;
    });
    expect(totalProducts).toBe(1000);
  });
});
