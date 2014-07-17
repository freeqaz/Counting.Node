// Task worker in Node.js
// Connects PULL socket to tcp://localhost:5557
// Collects workloads from ventilator via that socket
// Connects PUSH socket to tcp://localhost:5558
// Sends results to sink via that socket

var zmq = require('zmq'),
    receiver = zmq.socket('pull'),
    sender = zmq.socket('push');

receiver.on('message', function (buf) {
    var taskString = buf.toString();

    var tasks = taskString.split(",");

    var job = tasks[0];

    console.log("job" + job);

    var result = { Data: {} };

    // Do the work.
    for (var i = 1; i < tasks.length; i++) {
        // Is there a better way to do this?
        if (result.Data[tasks[i]] !== undefined) {
            result.Data[tasks[i]] += 1;
        } else {
            result.Data[tasks[i]] = 1;
        }
    }

    // Push to Sink
    sender.send(JSON.stringify(result));
});

receiver.connect('tcp://localhost:5557');
sender.connect('tcp://localhost:5558');

process.on('SIGINT', function () {
    receiver.close();
    sender.close();
    process.exit();
});
