package cn.edu360.Test;



import com.sun.mail.iap.ByteArray;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class TEst {

    public static void main(String[] args) throws IOException {

        List list = new ArrayList<>();
        list.add(2 );
        list.add(1 );
        list.add(1 );


        List collect = (List) Stream.concat(list.stream(), list.stream()).collect(Collectors.toList());

        collect.forEach(System.out::println);


        File file = new File("F:\\2.png");
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(new File("F:\\1.png"));
            FileOutputStream fileOutputStream1 = new FileOutputStream(new File("F:\\111.png"));
            FileInputStream fileInputStream = new FileInputStream(file);

            byte[] v= new byte[1027];
            int length;

            ByteArrayOutputStream byteArrayInputStream = new ByteArrayOutputStream();
            while((length=fileInputStream.read(v))!=-1){

                  byteArrayInputStream.write(v,0,length);

            }



        } catch (FileNotFoundException e) {


        }



        Father father = new Son();
        father.

    }
}
