"""
Created on Sun May 29  2016
@author: Pranay Kalva
"""

from mrjob.job import MRJob
from itertools import combinations
from math import sqrt   

class filmReview(MRJob):
    i=0
    list =[]
    def steps(self):
        return[
        self.mr(mapper=self.mapper1,
                reducer=self.reducer1),
        self.mr(mapper=self.mapper2,
                reducer=self.reducer2)
                ]
    def mapper1(self, line_no, line):
# Splitting the input with | 
        s = line.split("|")
#Yielding Movie name , (reviewer name and review) as output from mapper
        yield(s[0],(s[1],float(s[2])))

    def reducer1(self,key,value):
#Yielding the output from reducer with list value so that we can use combinations on it
        yield(key,list(value))

    def mapper2(self,key,ratings):
#Using combinations and izip combining respective movies from the same reviewer 
        for item1, item2 in combinations(ratings, 2):
            yield (item1[0], item2[0]), \
                     (item1[1], item2[1])
                    
#Using the formula for pearson coefficient as 
#     (E(x.y)-E(x).E(y)/n)/(sqrt(E(x^2)-E(x).E(x)/n))*sqrt(E(y^2)-E(y).E(y)/n))
# from  http://www.stat.wmich.edu/s216/book/node122.html and https://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient
                    
    def normalized_correlation(self, n, sum_xy, sum_x, sum_y, sum_xx, sum_yy):
        numerator = (n * sum_xy - sum_x * sum_y)
        denominator = sqrt(n * sum_xx - sum_x * sum_x) * sqrt(n * sum_yy - sum_y * sum_y)
        return numerator / denominator if denominator else 0

#Reducer to find pierson coefficient   
    def reducer2(self, pair_key, lines):
        sum_xx, sum_xy, sum_yy, sum_x, sum_y, n = (0.0, 0.0, 0.0, 0.0, 0.0, 0)
        item_pair = pair_key
        item_xname, item_yname = item_pair
        for item_x, item_y in lines:
            sum_xx += item_x * item_x
            sum_yy += item_y * item_y
            sum_xy += item_x * item_y
            sum_y += item_y
            n += 1
            sum_x += item_x
        similarity = self.normalized_correlation(n, sum_xy, sum_x, sum_y, \
                sum_xx, sum_yy)
        yield ("The similarity between "+item_xname+ " and "+item_yname +" is " ), (similarity, n)


if __name__ == '__main__':
    filmReview.run()    
