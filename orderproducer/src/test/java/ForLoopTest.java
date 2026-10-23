


public class ForLoopTest {

    public static void main(String[] args) {

        int[] intArray = {1,2,3,4,5,6};
        int count =0;
        for(int number:intArray)
        {
            count ++; // 3,6

            if(count%3==0)
            {
                System.out.println(number);
            }


//            count ++;     // 1,4,


        }
    }
}
