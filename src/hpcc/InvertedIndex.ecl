IMPORT Std;

//Get all subfile names
Dataset(Std.File.FsLogicalFileNameRecord) fileList := NOTHOR(STD.File.SuperFileContents('~hpcc::wiki::superfile'));

// Prepend filenames with "~" 
fileList2 := PROJECT(fileList, 
					 TRANSFORM(Std.File.FsLogicalFileNameRecord, 
			  				   SELF.name :='~' + LEFT.name
							  )
					);
				
//save file to storage so that it can be shared by the whole cluster	
OUTPUT(fileList2,,'~hpcc::ii::tmp_filelist',OVERWRITE);


FilelistLayOut := RECORD
	string filename;
END;

fileList3 := Distribute(DATASET('~hpcc::ii::tmp_filelist', Std.File.FsLogicalFileNameRecord, THOR));

//Map filename to corresponding dataset
LineLayout := RECORD
   STRING line;
END;

FileDsLayout := RECORD
   string filename;
   DATASET(LineLayout) ds;
END;

FileDsLayout mapFileDs(Std.File.FsLogicalFileNameRecord pInput)
      := TRANSFORM
     SELF.filename := pInput.name;
     SELF.ds := DATASET(pInput.name, LineLayout, CSV(separator(''),quote('')));
END;

fileDs := Project(fileList3, mapFileDs(Left));

//define function for spliting one file's words
WordLayout := RECORD
   STRING word;
END;


LineWordsLayout := RECORD
   DATASET(WordLayout)   words;
END;

LineWordsLayout getWords(LineLayout pInput)
      := TRANSFORM
     SELF.words := DATASET(Std.Str.SplitWords(pInput.line, ' '), WordLayout)
END;

//Split words for each file
FileWordsLayout := RECORD
   string filename;
   DATASET(WordLayout)   words;
END;

FileWordsLayout mapFileWords(FileDsLayout pInput)
      := TRANSFORM
     SELF.filename := pInput.filename;
     SELF.words := PROJECT(pInput.ds, getWords(LEFT)).words;
END;
wordsDS := project(fileDs, mapFileWords(LEFT));


//Remove duplicate words per file
FileWordsLayout dedupFileWords(FileWordsLayout pInput)
      := TRANSFORM
     SELF.filename := pInput.filename;
     SELF.words := DEDUP(SORT(pInput.words, word), Left.word = Right.word);
END;
dedupWordsDS := project(wordsDS, dedupFileWords(LEFT)); 


//Normalize filename -> dataset(word) to filename -> word
FileWordLayout := RECORD
   String word;
   string filename;
END;

FileWordLayout normFileNameWords(FileWordsLayout r, Integer c) 
		:= TRANSFORM
		SELF.filename := r.filename;
		self.word := r.words[c].word;
END;

normalizedWords := Normalize(dedupWordsDS, count(Left.words), normFileNameWords(left, counter));


//Save results
//OUTPUT(normalizedWords,,'~hpcc::ii::result',CSV(SEPARATOR('\t'), TERMINATOR('\n')),OVERWRITE);
OUTPUT(normalizedWords,,'~hpcc::ii::result',OVERWRITE);




