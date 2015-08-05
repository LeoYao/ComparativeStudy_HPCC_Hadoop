
IMPORT std;
IMPORT * from  ML;
IMPORT * from ML.Cluster;
IMPORT * from ML.Types;


//Map filename to corresponding dataset
LineLayout := RECORD
	STRING movie_id;
	STRING50000 line;
END;

original_file := Distribute(Dataset('~hpcc::movie::kmeans_smpl1000', LineLayout, CSV(separator(':'),quote(''))));

UseridRatingLayOut := Record
	string50000 userid_rating;
End;


UseridRatingsLayOut := Record
	string movie_id;
	dataset(UseridRatingLayOut) userid_ratings;
End;



UseridRatingsLayOut getUseridRatings(LineLayout pInput) := TRANSFORM
	Self.movie_id := pInput.movie_id;
    SELF.userid_ratings := DATASET(Std.Str.SplitWords(pInput.line, ','), UseridRatingLayOut)
END;


userid_ratings_recordset := Project(original_file, getUseridRatings(Left));

RatingLayout := Record
	integer rating;
End;


RatingsLayout := Record
	string movie_id;
	Dataset(RatingLayout) ratings;
End;

RatingLayout getRating(UseridRatingLayOut pInput) := TRANSFORM
     SELF.rating := (integer)(Std.Str.SplitWords(pInput.userid_rating, '_')[2]);
END;

RatingsLayout getRatings(UseridRatingsLayOut pInput) := TRANSFORM
	self.movie_id := pInput.movie_id;
    SELF.ratings := Project(pInput.userid_ratings, getRating(Left))[1..10];
END;


ratings_recordset := Project(userid_ratings_recordset, getRatings(Left));

MovieRatingsLayout := Record
	integer movie_id;
	integer rating1;
	integer rating2;
	integer rating3;
	integer rating4;
	integer rating5;
	integer rating6;
	integer rating7;
	integer rating8;
	integer rating9;
	integer rating10;
End;

MovieRatingsLayout TransformRating(RatingsLayout pInput) :=
	Transform
		self.movie_id := (integer)pInput.movie_id;
		self.rating1 := pInput.ratings[1].rating;
		self.rating2 := pInput.ratings[2].rating;
		self.rating3 := pInput.ratings[3].rating;
		self.rating4 := pInput.ratings[4].rating;
		self.rating5 := pInput.ratings[5].rating;
		self.rating6 := pInput.ratings[6].rating;
		self.rating7 := pInput.ratings[7].rating;
		self.rating8 := pInput.ratings[8].rating;
		self.rating9 := pInput.ratings[9].rating;
		self.rating10 := pInput.ratings[10].rating;
End;

finalInput := Project(ratings_recordset, TransformRating(Left));

MovieRatingsLayout GetFirstThreeRecords(MovieRatingsLayout pInput, Integer c) :=
	Transform, Skip(c >= 17)
		self := pInput;
End;

fileSample := Project(finalInput, GetFirstThreeRecords(Left, Counter));

ml.ToField(finalInput, fI2);
ml.ToField(fileSample, fS2);

//Run K-Means for, at most, 1 iterations and stop if delta < 0.3 between iterations
fX3 := Kmeans(fI2, fS2, 1, 0.0);

//Convert the final centroids to the original layout
ml.FromField(fX3.result(), MovieRatingsLayout, finalCentroids);
//ml.FromField(fX3.Allegiance(2));

//Display the results
//OUTPUT(fX3.Allegiances());
output(fX3.Allegiances(),, '~hpcc::kmeans::result', overwrite);
output(finalCentroids,, '~hpcc::kmeans::finalCentroids', overwrite);