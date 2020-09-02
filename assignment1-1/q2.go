package cos418_hw1_1

import (
    "bufio"
    "io"
    "log"
    "os"
    "strconv"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
    sum := 0
    for num := range nums {
        sum += num
    }
    out <- sum
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
    // Opening file and logging if there are any errors.
    file, err := os.Open(fileName)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    messages := make(chan int, num)
    out := make(chan int, num)
    // Creating `num` workers
    for i := 0; i < num; i++ {
        go sumWorker(messages, out)
    }

    // Reading the file and streaming the information to the producer.
    integers, err := readInts(file)
    for _, n := range integers {
        messages <- n
    }
    close(messages)

    // Adding all results to a single result for output.
    var sum int = 0
    for i := 0; i < num; i++ {
        sum += <-out
    }

    return sum
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
    scanner := bufio.NewScanner(r)
    scanner.Split(bufio.ScanWords)
    var elems []int
    for scanner.Scan() {
        val, err := strconv.Atoi(scanner.Text())
        if err != nil {
            return elems, err
        }
        elems = append(elems, val)
    }
    return elems, nil
}
