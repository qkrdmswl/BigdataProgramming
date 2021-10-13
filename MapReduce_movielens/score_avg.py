from mrjob.job import MRJob
from mrjob.step import MRStep


class PopularMovie(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.map_rating_count,
                reducer=self.reduce_filter_rating),
            MRStep(reducer=self.reduce_rating_avg),
            MRStep(reducer=self.reduce_sort)
        ]


    def map_rating_count(self, _, line):
        # userId, movieId, rating, timestamp
        data = line.split(',')
        if data[0] != 'userId':
            yield data[1], data[2]  # movieId와 rating을 mapping하여 전달


    def reduce_filter_rating(self, movie_id, values):
        # values = [3.5, 2.0, 4.0 ,,, ] 의 형태로, 각 movieId의 rating 값들이 저장되어 있다.
        # values는 generator이기 때문에 이를 전달하기 위해서는 list 형태로 바꿔줘야 함.
        # 따라서 data라는 list 변수 생성
        data = []
        for i in values:      # i는 rating 개별 값. 이것들 하나씩 data 리스트에 넣어준다.
            data.append(i)
            
        # 만약 data 리스트의 길이가 10보다 크다면, 리뷰의 수가 10개 초과라는 뜻이므로 이에 해당하는 movieId와 그것의 rating 값이 저장된 리스트 data를 반환한다.
        if len(data) > 10:
            yield movie_id, data


    
    def reduce_rating_avg(self, movie_id, counts):
        # cnt : rating 개수를 세기 위해 만든 변수
        # sum : rating 총합을 저장하기 위해 만든 변수
        cnt = 0
        sum = 0
        for i in counts:
            cnt = len(i)
            for j in i:
                sum = sum + float(j)
            yield None, (str(sum/cnt), movie_id)   # key는 None, Value는 avg와 movieId를 묶어서 전달


    # 평점 내림차순으로 정렬해주기
    def reduce_sort(self, key, values):
        for rating_avg, movie in sorted(values, reverse=True):   # 앞서 받아온 values(즉, avg와 movieId가 묶여있는 것)를 reverse=True로 내림차순 정렬해주고, 이거에서 avg와 movieId를 반환해준다.
            yield movie, rating_avg

   

if __name__ == '__main__':
    PopularMovie.run()