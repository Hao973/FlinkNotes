package hao.demo;

public class WordWithCount {
    public String word;
    public long count;
    public WordWithCount(){}
    public WordWithCount(String word, long count){
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return "WordWithCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}
