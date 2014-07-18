// Task ventilator in node.js
// Binds PUSH socket to tcp://localhost:5557
// Sends batch of tasks to workers via that socket.

var zmq = require('zmq');
process.stdin.resume();
require('tty').setRawMode(true);

// Socket to send messages on
var sender = zmq.socket('push');
sender.bindSync("tcp://*:5557");

var sink = zmq.socket('push');
sink.bindSync("tcp://*:9001")

var i = 0, total_msec = 0;

function work() {
    console.log("Sending tasks to workers…");

    var getRandomInt = function(min, max) {
        return Math.floor(Math.random() + (max - min) + min);
    }

    // This is the size per 'batch'.
    // Using this emulate reading from a file too large to fit in memory.
    const batchSize = 268;

    // Set this to 200000 to simulate ~100gb of data.
    // Current set to ~10gb of data.
    const numberOfBatches = 20000;

    // 100gb = 107374182400 bytes.
    // 100gb / 20 = 53687091 words in file
    // 53687091 / 200000 = ~268 words per 'batch'.
    // That's around 50kb per chunk. Seems reasonable.
    // Using TCP right now, could use inproc too.

    // The first message is "0" and signals start of batch
    //sender.send("0");

    var currentBatch = 0;
    while (currentBatch < numberOfBatches) {

        // Just putting it all in a big string that's sent over ZeroMQ.
        // Separated by commas, but it's arbitrary.
        var workToSend = "";

        workToSend.concat("#" + currentBatch);

        // Random 'word' to send workers
        for (var s = 0; s < batchSize; s++) {
            // We can tweak the number of unique keys here...
            // More unique keys is more expensive to write to disk.
            // Anything greater than 3020000 results in huuuuge flushes to
            // The disk and gets pretty slow.
            // Anything less than that can remain in memory (with 4gb).
            // Best solution: Virtual Memory. You can keep pointers in memory
            // And not have manually handle flushing to the disk yourself.
            workToSend.concat(getRandomInt(1, 50000) + ",");
        }

        // Print out some output to show we're working.
        if (currentBatch % 10000 === 0)
        {
            var complete = (currentBatch / numberOfBatches * 100);
            console.log(complete + "% Complete pushing work");
        }

        sink.send("#" + currentBatch);
        sender.send(workToSend.slice(0, workToSend.length - 1));
    }

    // send 100 tasks
    //while (i < 100) {
    //    var workload = Math.abs(Math.round(Math.random() * 100)) + 1;
    //    total_msec += workload;
    //    process.stdout.write(workload.toString() + ".");
    //    sender.send(workload.toString());
    //    i++;
    //}
    //console.log("Total expected cost:", total_msec, "msec");

    sink.send("done");

    sender.close();
    process.exit();
};

console.log("Press enter when the workers are ready…");
process.stdin.on("data", function () {
    if (i === 0) {
        work();
    }
    process.stdin.pause();
});