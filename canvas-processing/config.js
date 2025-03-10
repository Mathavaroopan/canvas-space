const path = require('path');
const fs = require('fs');

const TMP_DIR = path.join(__dirname, '../canvas-core/tmp');
if (!fs.existsSync(TMP_DIR)) {
  fs.mkdirSync(TMP_DIR, { recursive: true });
}
const outputDir = path.join(__dirname, '../canvas-core/hls_output');
if (!fs.existsSync(outputDir)) {
  fs.mkdirSync(outputDir, { recursive: true });
}

module.exports = { TMP_DIR, outputDir };
