IMPORT Std;

LineLayout := RECORD
   STRING line;
END;

SrcDstLayout := RECORD
   STRING src;
   STRING dst;
END;

//Read file
linesDS := Distribute(DATASET
   (
      '~hpcc::adj::superfile',
      SrcDstLayout, 
      CSV(separator(','),quote(''))
   ));
   


SrcDstListLayout := RECORD
  STRING key;
  STRING dsts;
END;

DstSrcListLayout := RECORD
  STRING key;
  STRING srcs;
END;

//Merge based on source
SrcDstListLayout localMergeBySrc(SrcDstLayout l, SrcDstListLayout r) := TRANSFORM
  SELF.key := l.src;
  SELF.dsts := r.dsts + IF(r.dsts <> '', ',', '') + l.dst;
END;

//Due to implicit locality of AGGREGATE, a global combine function is necessary
SrcDstListLayout globalComineBySrc(SrcDstListLayout r1, SrcDstListLayout r2) := TRANSFORM
  SELF.key := r1.key;
  SELF.dsts := r2.dsts;
END;

//Merge based on destination
DstSrcListLayout localMergeByDst(SrcDstLayout l, DstSrcListLayout r) := TRANSFORM
  SELF.key := l.dst;
  SELF.srcs := r.srcs + IF(r.srcs <> '', ',', '') + l.src;
END;

//Due to implicit locality of AGGREGATE, a global combine function is necessary
DstSrcListLayout globalComineByDst(DstSrcListLayout r1, DstSrcListLayout r2) := TRANSFORM
  SELF.key := r1.key;
  SELF.srcs := r1.srcs + ',' + r2.srcs;
END;

srcResult := AGGREGATE(linesDS, SrcDstListLayout, localMergeBySrc(LEFT, RIGHT), globalComineBySrc(RIGHT1, RIGHT2), LEFT.src);
dstResult := AGGREGATE(linesDS, DstSrcListLayout, localMergeByDst(LEFT, RIGHT), globalComineByDst(RIGHT1, RIGHT2), LEFT.dst);


//Join the results of merge to generate the final result
ResultLayout := RECORD
  STRING key;
  STRING srcs;
  STRING dsts;
END;

ResultLayout doJoin(SrcDstListLayout l, DstSrcListLayout r) := TRANSFORM
	SELF.key := IF(l.key <> '', l.key, r.key);
	SELF.srcs := r.srcs;
	SELF.dsts := l.dsts;
END;

result := JOIN(srcResult, dstResult, LEFT.key=RIGHT.key, doJoin(LEFT,RIGHT), FULL OUTER);

OUTPUT(result,,'~hpcc::adj::result',OVERWRITE);
//OUTPUT(srcResult,,'~hpcc::adj::srcResult',OVERWRITE);
//OUTPUT(dstResult,,'~hpcc::adj::dstResult',OVERWRITE);

