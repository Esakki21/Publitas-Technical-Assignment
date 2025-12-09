const ONE_MEGA_BYTE = 1_048_576.0;

/**
 * Creates a batcher that groups products into batches under size limit
 * @param {number} maxSizeMB - Maximum batch size in megabytes
 * @param {Object} externalService - Service to call with batches
 */
export function createBatcher(maxSizeMB, externalService) {
  const maxSizeBytes = maxSizeMB * ONE_MEGA_BYTE;
  let currentBatch = [];
  let currentBatchSize = 0;
  let totalProductsProcessed = 0;
  let totalBatchesSent = 0;
  let totalSizeProcessed = 0;
  let totalFullBatchesSize = 0; // Track size of full batches only
  let fullBatchCount = 0; // Count of full batches

  /**
   * Calculates size of entire batch in bytes
   * Includes array brackets and commas
   */
  function calculateBatchSize(products) {
    if (products.length === 0) return 2; // Empty array "[]"

    const json = JSON.stringify(products);
    return Buffer.byteLength(json, "utf8");
  }

  /**
   * Estimates the size a product would add to the current batch
   * @param {Object} product - Product to estimate size for
   * @returns {number} Estimated size in bytes
   */
  function estimateProductSize(product) {
    const json = JSON.stringify(product);
    const productSize = Buffer.byteLength(json, "utf8");

    // Add 1 byte for comma separator (if not first product)
    const separator = currentBatch.length > 0 ? 1 : 0;

    return productSize + separator;
  }

  /**
   * Sends current batch to external service and resets
   */
  function sendBatch() {
    if (currentBatch.length === 0) return;

    const batchJSON = JSON.stringify(currentBatch);
    const batchSize = Buffer.byteLength(batchJSON, "utf8");

    // Update statistics
    totalProductsProcessed += currentBatch.length;
    totalBatchesSent += 1;
    totalSizeProcessed += batchSize;

    // Track full batches separately (batches that are close to max size)
    // Consider a batch "full" if it is > 95% of max size
    if (batchSize > maxSizeBytes * 0.95) {
      totalFullBatchesSize += batchSize;
      fullBatchCount += 1;
    }

    // Show progress
    console.log(
      `Sending batch ${totalBatchesSent}... (${
        currentBatch.length
      } products, ${(batchSize / ONE_MEGA_BYTE).toFixed(4)}MB)`
    );

    externalService.call(batchJSON);

    // Reset for next batch
    currentBatch = [];
    currentBatchSize = 0;
  }

  /**
   * Adds a product to the current batch
   * Sends batch if size limit would be exceeded
   */
  function addProduct(product) {
    // Estimate size impact of adding this product
    const estimatedAddition = estimateProductSize(product);
    const estimatedNewSize = currentBatchSize + estimatedAddition;

    // If adding this product would exceed limit, send current batch first
    if (estimatedNewSize > maxSizeBytes && currentBatch.length > 0) {
      sendBatch();
      // Start new batch with this product
      currentBatch = [product];
      // Recalculate exact size for new batch
      currentBatchSize = calculateBatchSize(currentBatch);
    } else {
      // Add to current batch
      currentBatch.push(product);

      // For first few products or periodically, recalculate exact size
      // This corrects any estimation drift
      if (currentBatch.length <= 3 || currentBatch.length % 100 === 0) {
        currentBatchSize = calculateBatchSize(currentBatch);
      } else {
        // Use estimated size for performance
        currentBatchSize = estimatedNewSize;
      }
    }
  }

  /**
   * Adds multiple products with progress updates
   */
  function addProducts(products) {
    const totalProducts = products.length;
    let processedCount = 0;

    console.log(`Processing ${totalProducts.toLocaleString()} products...\n`);

    for (const product of products) {
      addProduct(product);
      processedCount++;

      // Show progress every 5000 products
      if (processedCount % 5000 === 0) {
        const percentComplete = (
          (processedCount / totalProducts) *
          100
        ).toFixed(1);
        console.log(
          `Progress: ${processedCount.toLocaleString()}/${totalProducts.toLocaleString()} products (${percentComplete}%)`
        );
      }
    }

    console.log(
      `Completed processing all ${totalProducts.toLocaleString()} products\n`
    );
  }

  /**
   * Sends any remaining products in the current batch
   */
  function flush() {
    sendBatch();
    printSummary();
  }

  /**
   * Prints summary statistics
   */
  function printSummary() {
    console.log("\n" + "=".repeat(60));
    console.log("PROCESSING SUMMARY");
    console.log("=".repeat(60));
    console.log(
      `Total Products Processed: ${totalProductsProcessed.toLocaleString()}`
    );
    console.log(`Total Batches Sent:       ${totalBatchesSent}`);
    console.log(
      `Total Data Processed:     ${(totalSizeProcessed / ONE_MEGA_BYTE).toFixed(
        2
      )}MB`
    );

    if (totalBatchesSent > 0) {
      const avgBatchSize =
        totalSizeProcessed / totalBatchesSent / ONE_MEGA_BYTE;
      const overallUtilization = (avgBatchSize / maxSizeMB) * 100;

      console.log(`Average Batch Size:       ${avgBatchSize.toFixed(2)}MB`);
      console.log(
        `Average Products/Batch:   ${Math.round(
          totalProductsProcessed / totalBatchesSent
        )}`
      );
      console.log(
        `Overall Utilization:      ${overallUtilization.toFixed(2)}%`
      );

      // Show full batch utilization separately
      if (fullBatchCount > 0) {
        const fullBatchAvg =
          totalFullBatchesSize / fullBatchCount / ONE_MEGA_BYTE;
        const fullBatchUtilization = (fullBatchAvg / maxSizeMB) * 100;
        console.log(
          `Full Batches (${fullBatchCount}):       ${fullBatchAvg.toFixed(
            2
          )}MB avg, ${fullBatchUtilization.toFixed(2)}% utilization`
        );
      }

      // Show partial batch info if exists
      const partialBatches = totalBatchesSent - fullBatchCount;
      if (partialBatches > 0) {
        const partialSize =
          (totalSizeProcessed - totalFullBatchesSize) / ONE_MEGA_BYTE;
        console.log(
          `Partial Batches (${partialBatches}):    ${partialSize.toFixed(
            2
          )}MB total`
        );
      }
    } else {
      console.log("No batches were sent.");
    }

    console.log("=".repeat(60));
  }

  return {
    addProduct,
    addProducts,
    flush,
  };
}
