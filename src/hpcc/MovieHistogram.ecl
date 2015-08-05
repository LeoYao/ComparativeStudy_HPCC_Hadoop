IMPORT Std;

//Map filename to corresponding dataset
LineLayout := RECORD
	STRING movie_id;
   STRING50000 line;
END;


original_file := Distribute(Dataset('~hpcc::movie::superfile', LineLayout, CSV(separator(':'),quote(''))));

UseridRatingLayOut := Record
	string50000 userid_rating;
End;

UseridRatingsLayOut := Record
	dataset(UseridRatingLayOut) userid_ratings;
End;

UseridRatingsLayOut getUseridRatings(LineLayout pInput)
      := TRANSFORM
     SELF.userid_ratings := DATASET(Std.Str.SplitWords(pInput.line, ','), UseridRatingLayOut)
END;

userid_ratings_recordset := Project(original_file, getUseridRatings(Left), LOCAL);

RatingLayout := Record
	decimal10 rating;
End;

RatingsLayout := Record
	Dataset(RatingLayout) ratings;
End;

RatingLayout getRating(UseridRatingLayOut pInput)
      := TRANSFORM
     SELF.rating := (Decimal10)(Std.Str.SplitWords(pInput.userid_rating, '_')[2]);
END;

RatingsLayout getRatings(UseridRatingsLayOut pInput)
      := TRANSFORM
     SELF.ratings := Project(pInput.userid_ratings, getRating(Left), LOCAL);
END;

ratings_recordset := Project(userid_ratings_recordset, getRatings(Left), LOCAL);


AvgRatingLayout := Record
	Real4 avg_rating;
End;

AvgRatingLayout ComputeAvgRating(RatingsLayout pInput) := 
	Transform
		Self.avg_rating := AVE(pInput.ratings, pInput.ratings.rating);
End;

Avg_Ratings := Project(ratings_recordset, ComputeAvgRating(Left));

MyRecordSet1 := Avg_Ratings(avg_rating >= 1.00   AND avg_rating < 1.50);
MyRecordSet2 := Avg_Ratings(avg_rating >= 1.50   AND avg_rating < 2.00);
MyRecordSet3 := Avg_Ratings(avg_rating >= 2.00   AND avg_rating < 2.50);
MyRecordSet4 := Avg_Ratings(avg_rating >= 2.50   AND avg_rating < 3.00);
MyRecordSet5 := Avg_Ratings(avg_rating >= 3.00   AND avg_rating < 3.50);
MyRecordSet6 := Avg_Ratings(avg_rating >= 3.50   AND avg_rating < 4.00);
MyRecordSet7 := Avg_Ratings(avg_rating >= 4.00   AND avg_rating < 4.50);
MyRecordSet8 := Avg_Ratings(avg_rating >= 4.50   AND avg_rating < 5.00);

//Output(MyRecordSet2,, '~hpcc::movie_hist::result', Overwrite);
valRec1 := COUNT(MyRecordSet1);
Output(valRec1);
valRec2 := COUNT(MyRecordSet2);
Output(valRec2);

valRec3 := COUNT(MyRecordSet3);
Output(valRec3);
valRec4 := COUNT(MyRecordSet4);
Output(valRec4);
valRec5 := COUNT(MyRecordSet5);
Output(valRec5);
valRec6 := COUNT(MyRecordSet6);
Output(valRec6);
valRec7 := COUNT(MyRecordSet7);
Output(valRec7);
valRec8 := COUNT(MyRecordSet8);
Output(valRec8);

//////////


 