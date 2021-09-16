const { writeHeapSnapshot } = require("v8");

class HugeObj {
  constructor() {
    this.hugeData = Buffer.alloc((1 << 20) * 50, 0);
  }
}

module.exports.data = new HugeObj();

writeHeapSnapshot();