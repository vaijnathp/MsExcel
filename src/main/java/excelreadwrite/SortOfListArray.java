package excelreadwrite;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by vaijnathp on 9/21/2017.
 */
public class SortOfListArray {
    public static void main(String[] args) {
        List<Integer> listArray[] = new List[4];

        List<Integer> list = new ArrayList<>();
        list.add(3);
        list.add(2);
        list.add(1);
        list.add(1);
        listArray[0] = list;

        List<Integer> list1 = new ArrayList<>();
        list1.add(1);
        list1.add(2);
        list1.add(2);
        list1.add(1);
        listArray[1] = list1;

        List<Integer> list2 = new ArrayList<>();
        list2.add(2);
        list2.add(1);
        list2.add(3);
        list2.add(1);
        listArray[2] = list2;

        List<Integer> list3 = new ArrayList<>();
        list3.add(3);
        list3.add(1);
        list3.add(4);
        list3.add(1);
        listArray[3] = list3;

        Field fieldArr[] = new Field[4];
        fieldArr[0] = new Field(0, "asc");
        fieldArr[1] = new Field(1, "asc");
        fieldArr[2] = new Field(2, "asc");
        fieldArr[3] = new Field(3, "desc");


        for (List<Integer> l : listArray
                ) {
            l.forEach(e -> System.out.print(e + "\t"));
            System.out.println();
        }
        System.out.println("**************************");
        sort(listArray, 0, fieldArr);
        for (List<Integer> l : listArray
                ) {
            l.forEach(e -> System.out.print(e + "\t"));
            System.out.println();
        }
    }



    private static void sort(List<Integer>[] listArray, int fieldArrIndex, Field[] fieldArr) {
        int n = listArray.length;
        int index = fieldArr[fieldArrIndex].index;
        String order = fieldArr[fieldArrIndex].order;
        List<Integer> temp = null;

        for (int i = 0; i < n; i++) {
            for (int j = 1; j < (n - i); j++) {
                if (listArray[j - 1].get(index) == listArray[j].get(index)&& fieldArrIndex < fieldArr.length-1) {
                    sort(listArray, fieldArrIndex + 1, fieldArr);
                } else {

                    if (order.equals("asc")) {
                        if (listArray[j - 1].get(index) > listArray[j].get(index)) {
                            temp = listArray[j - 1];
                            listArray[j - 1] = listArray[j];
                            listArray[j] = temp;
                        }
                    } else {
                        if (listArray[j - 1].get(index) < listArray[j].get(index)) {
                            temp = listArray[j - 1];
                            listArray[j - 1] = listArray[j];
                            listArray[j] = temp;
                        }
                    }}


                }
            }
        }
    }

