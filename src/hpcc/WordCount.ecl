IMPORT Std;

//Read file
LineLayout := RECORD
   STRING line;
END;

linesDS := DATASET
   (
      '~hpcc::wiki::superfile',
      //'~ hpcc::wiki::file20',
      LineLayout, 
      CSV(separator(''),quote(''))
   );

//Split each line into words
WordLayout := RECORD
   STRING word;
END;

LineWordsLayout := RECORD
   DATASET(WordLayout)   words;
END;

wordsTemp := PROJECT
   (
      linesDS,
      TRANSFORM
         (
            LineWordsLayout,
            SELF.words := DATASET(Std.Str.SplitWords(LEFT.line, ' '), WordLayout)
         ), LOCAL
   );

//Extract all words
wordsDS := wordsTemp.words;

//Count words
WordCountLayout := RECORD
   wordsDS.word;
   wordCount := COUNT(GROUP);
END;

wordCountTable := TABLE(wordsDS, WordCountLayout, word);

//Save results
//OUTPUT(wordCountTable,,'~hpcc::wc::wordCount',CSV(SEPARATOR('\t'), TERMINATOR('\n')),OVERWRITE);
OUTPUT(wordCountTable,,'~hpcc::wc::result',OVERWRITE);
//OUTPUT(wordsDS,,'~hpcc::wc::wordsDS',OVERWRITE);
