IMPORT Std;


LineLayout := RECORD
   STRING line;
END;

//Read file
linesDS := Distribute(DATASET
   (
      '~hpcc::sj::superfile',
      //'~hpcc::sj::file1',
      LineLayout, 
      CSV(separator(''),quote(''))
   ), HASH(line));

//Remove duplication
dedupedDS := DEDUP(SORT(linesDS, line, LOCAL), Left.line = Right.line, LOCAL);

NMinusOneLayout := RECORD
	string nMinusOneWords;
	string nWord;
END;

//Split lines into first n-1 words and the nth word
NMinusOneLayout splitLine(LineLayout pInput)
      := TRANSFORM
      Integer commaNum := Std.Str.FindCount(pInput.line, ',');
      Integer lastCommaPos := Std.Str.Find(pInput.line, ',', commaNum);
      SELF.nMinusOneWords := pInput.line[1..lastCommaPos-1];
      SELF.nWord := pInput.line[lastCommaPos+1..Length(pInput.line)];
END;

nMinusOneDS := Distribute(Project(dedupedDS, splitLine(left), LOCAL), Hash(nMinusOneWords));


ResultLayout := RECORD
  STRING key;
  STRING value;
END;

//SelfJoin
ResultLayout localMergeJoin(NMinusOneLayout l, ResultLayout r) := TRANSFORM
  SELF.key := l.nMinusOneWords;
  SELF.value := r.value + IF(r.value <> '', ',', '') + l.nWord;
END;

//Due to implicit locality of AGGREGATE, a global combine function is necessary
ResultLayout globalComine(ResultLayout r1, ResultLayout r2) := TRANSFORM
  SELF.key := r1.key;
  SELF.value := r1.value + ',' + r2.value;
END;

tmpResult := AGGREGATE(nMinusOneDS, ResultLayout, localMergeJoin(LEFT, RIGHT), globalComine(RIGHT1, RIGHT2), LEFT.nMinusOneWords, LOCAL);

//Remove records that do not have common items
ResultLayout removeNoCommon(ResultLayout pInput)
	:= TRANSFORM, SKIP(NOT STD.Str.Contains(pInput.value, ',', true))
	SELF := pInput;
END;

result := Project(tmpResult, removeNoCommon(left), LOCAL);

//output(result);
	
//Save file
//OUTPUT(result,,'~hpcc::sj::result',CSV(SEPARATOR('\t'), TERMINATOR('\n')),OVERWRITE);
OUTPUT(result,,'~hpcc::sj::result',OVERWRITE);
	