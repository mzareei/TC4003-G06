package cos418_hw1_1

import (
    "bufio"
    "fmt"
    "os"
    "regexp"
    "sort"
    "strings"
)


// Remove all special characters of a token using a regular expression and
// converts all alphabetic characters to lower letters.
//  r: regular expression used to remove special characters from token.
//  word: token to be normalized.
func normalizeWord(r *regexp.Regexp, word string) string {
    alphanumericWrd := r.ReplaceAllString(word, "")
    return strings.ToLower(alphanumericWrd)
}


// Validate if a token qualifies as a word or not.
//  word: token to be evaluated.
//  charThreshold: character threshold for whether a token qualifies as a word,
//      e.g. charThreshold = 5 means "apple" is a word but "pear" is not.
func validateWord(word string, charThreshold int) bool {
    return len(word) >= charThreshold
}


// Find the top K most common words in a text document.
//  path: location of the document
//  numWords: number of words to return (i.e. k)
//  charThreshold: character threshold for whether a token qualifies as a word,
//      e.g. charThreshold = 5 means "apple" is a word but "pear" is not.
// Matching is case insensitive, e.g. "Orange" and "orange" is considered the same word.
// A word comprises alphanumeric characters only. All punctuations and other characters
// are removed, e.g. "don't" becomes "dont".
// You should use `checkError` to handle potential errors.
func topWords(path string, numWords int, charThreshold int) []WordCount {

    // Opening file and logging if there are any errors.
    file, err := os.Open(path)
    checkError(err)
    defer file.Close()

    scanner := bufio.NewScanner(file)
    scanner.Split(bufio.ScanWords)

    // Hashmap to count word appeareances.
    var wordsCount map[string]int
    wordsCount = make(map[string]int)

    r, _ := regexp.Compile("[^0-9a-zA-Z]+")
    
    // Normalizing words and counting repetitions.
    // Normalize: Remove special characters to words.
    for scanner.Scan() {
        word := scanner.Text()
        normalizedWord := normalizeWord(r, word)
        if validateWord(normalizedWord, charThreshold) {
            wordsCount[normalizedWord]++
        }
    }

    // Converting all results into WordCount objects.
    var wcList []WordCount
    for word, count := range wordsCount {
        var wc WordCount
        wc.Word = word
        wc.Count = count
        wcList = append(wcList, wc)
    }

    sortWordCounts(wcList)

    // Obtaining only the number of words that are requeted.
    // If the requested number of words is bigger than the ones in the list
    // then all list is returned.
    var result []WordCount
    for i := 0; i < numWords && i < len(wcList); i++ {
        result = append(result, wcList[i])
    }

    if err := scanner.Err(); err != nil {
        checkError(err)
    }

    return result
}

// A struct that represents how many times a word is observed in a document.
type WordCount struct {
    Word  string
    Count int
}

func (wc WordCount) String() string {
    return fmt.Sprintf("%v: %v", wc.Word, wc.Count)
}

// Helper function to sort a list of word counts in place.
// This sorts by the count in decreasing order, breaking ties using the word.
// DO NOT MODIFY THIS FUNCTION!
func sortWordCounts(wordCounts []WordCount) {
    sort.Slice(wordCounts, func(i, j int) bool {
        wc1 := wordCounts[i]
        wc2 := wordCounts[j]
        if wc1.Count == wc2.Count {
            return wc1.Word < wc2.Word
        }
        return wc1.Count > wc2.Count
    })
}
