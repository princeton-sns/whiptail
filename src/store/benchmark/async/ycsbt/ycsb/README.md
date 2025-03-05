# Read [the java code of YCSB](https://github.com/brianfrankcooper/YCSB/blob/master/core/src/main/java/site/ycsb/workloads/CoreWorkload.java)

# Detailed comments from Zhuoyue

by the way, a few slightly more detailed comments on the ycsb generator (@ commit fcdde84) you're writing:

- It would be great if you can remove the pure virtual interface NumberGenerator since this would add substantial virtual function call overhead.

- If you're assuming the random value is a string, you cannot do return a double as a string (unlike java). You would have to invoke std::to_string(). **However**, a more general comment is instead of doing so, you should probably refactor the generator interface using the STL-style random number generation logic where distribution is separately represented from the random bit generators. See [here](https://en.cppreference.com/w/cpp/numeric/random): 

- It would be great if you could follow the Google C++ style guide, where header and source files for the same module are put under the same directory without include or src subdirectories. For each header file, if you're using the traditional ifdef header guard, please use the path to the file as the macro name: see [here](https://google.github.io/styleguide/cppguide.html#The__define_Guard)


# Notes from Haonan

I wrote some notes on the workload generator task you will be working on, [here](https://docs.google.com/document/d/1kzAYzQ6QKpnqKADVBBuGMX4_FSjIyWkm83H7v9R59x0/edit)

## What is this part?

1. If you have read the [PORT paper](https://www.usenix.org/system/files/osdi20-lu.pdf),([Lu et al.](zotero://select/library/items/4PEA9YYS)) especially its evaluation section, and tried to play around with its evaluation experiments in the Appendix A, you’d have basic understanding of systems evaluation process.

2. Systems evaluation basically consists of two parts: workload generator and the system.

3. In an evaluation experiment, the workload generator constructs operations, e.g., reads and writes, and send these operations to the system, which will execute these operations. During the experiment, performance statistics are recorded, e.g., throughput and latency.

4. Shengzhou and Xixian’s job is focused on the workload generator, and Shawn’s job is to build the connection between the workload generator and the system so that the workload generator can work with many different systems with minimal engineering effort.


## What is to be done?

1. Implement the workload generator, from the basic core features to later adding more complex logic.

2. At a high level, the flow is to

    1. generate an operation,

    2. Send the operation to the system

3. Generate an operation

    1. Decide the type of the operation, e.g., get, put, batched_gets, and batched_puts. For now, you can focus on get and put, i.e., the simple read and write ops on key-value pairs.

        1. To decide operation type, you need to do it based on configuration: read-to-write ratio. For instance, if the ratio of read to write is 1:1, then there is 50% chance this op is a put and 50% chance this op is a get. Similarly, if the ratio is 3:1, then there is 75% chance for this operation to be a get.

        2. You should have a separate configuration file that has all the configuration parameters, including read-to-write ratio.

        3. You may find YCSB’s workload A,B,C… are basically varying the read-to-write ratios, then you do not need to implement easy of them separately. Instead, allow this read-to-write ratio parameter, and generate workloads accordingly.

    2. Construct the read or write operation

        1. Get a key to read or write

            1. Get a key based on workload distribution, e.g., if the workload is uniform and there are 1M keys, then there is an equal chance to get any key. If the workload is skewed, e.g., Zipfian constant = 0.99, then 1% of the keys are really popular, and there are 99% chance to get a key within this 1%.

            2. Then you need to take in the parameters for the total number of keys, and zipfian constant. Note that zipafian constant of 0 means the workload is uniform.

        2. Make the value for a put operation

            1. For this, we need user input to let us know the setup and type of values. For now, you can assume a random string.

            2. You may want to code a way to allow configure values of different sizes, e.g., if the framework want to test value sizes of 1B, 500B, 1KB, etc.

    3. Write the read and write operation logic

        1. This should be provided by the user, or linked to the system’s read/write handlers. For now, you can make a dummy function for this, e.g., in the function you can simply log a printout statement “I’m executing a get (or put), key = xxx, value = xxx.”