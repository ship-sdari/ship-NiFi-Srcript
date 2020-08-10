
//package com.sdari.groovy.main

class Calculation {

    public static void main(String[] args) {

        GroovyClassLoader loader = new GroovyClassLoader();
        Class aClass = loader.parseClass(new File("F:\\IDEA\\nifi\\nifi-script\\nifi-script\\ship-nifi-srcipt\\src\\main\\script\\com\\sdari\\groovy\\vo\\AmsInnerKeyVo.groovy"));

            GroovyObject instance = (GroovyObject) aClass.newInstance();
            print(instance.ge_fo1_total)


    }}