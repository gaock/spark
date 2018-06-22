[33mcommit c6abddb97f92482f1d294a747547c10019171fab[m[33m ([m[1;36mHEAD -> [m[1;32mriffle-spark-v2.2.1[m[33m)[m
Author: gaock <gaochuanp@gmail.com>
Date:   Wed Jun 20 11:29:17 2018 +0800

    conf.getBoolean(spark.conf.isUseRiffle)

[33mcommit b8c7df40220f5e185c028c18890534cb9142288d[m[33m ([m[1;31morigin/riffle-spark-v2.2.1[m[33m, [m[1;32mriffle2[m[33m)[m
Author: gaock <gaochuanp@gmail.com>
Date:   Tue Jun 19 22:24:17 2018 +0800

    riffle Debug

[33mcommit 6076dacb84f2fd3b2fead4fcc9272c353588400f[m
Author: gaock <gaochuanp@gmail.com>
Date:   Tue Jun 19 21:42:40 2018 +0800

    riffle debug

[33mcommit e30e2698a2193f0bbdcd4edb884710819ab6397c[m[33m ([m[1;33mtag: v2.2.1-rc2[m[33m, [m[1;33mtag: v2.2.1[m[33m)[m
Author: Felix Cheung <felixcheung@apache.org>
Date:   Fri Nov 24 21:11:35 2017 +0000

    Preparing Spark release v2.2.1-rc2

[33mcommit c3b5df22aa34001d8412456ee346042b6218c664[m
Author: Felix Cheung <felixcheung@apache.org>
Date:   Fri Nov 24 12:06:57 2017 -0800

    fix typo

[33mcommit b606cc2bdcfb103b819dac65cc610283cfa0ded5[m
Author: Jakub Nowacki <j.s.nowacki@gmail.com>
Date:   Fri Nov 24 12:05:57 2017 -0800

    [SPARK-22495] Fix setup of SPARK_HOME variable on Windows
    
    ## What changes were proposed in this pull request?
    
    This is a cherry pick of the original PR 19370 onto branch-2.2 as suggested in https://github.com/apache/spark/pull/19370#issuecomment-346526920.
    
    Fixing the way how `SPARK_HOME` is resolved on Windows. While the previous version was working with the built release download, the set of directories changed slightly for the PySpark `pip` or `conda` install. This has been reflected in Linux files in `bin` but not for Windows `cmd` files.
    
    First fix improves the way how the `jars` directory is found, as this was stoping Windows version of `pip/conda` install from working; JARs were not found by on Session/Context setup.
    
    Second fix is adding `find-spark-home.cmd` script, which uses `find_spark_home.py` script, as the Linux version, to resolve `SPARK_HOME`. It is based on `find-spark-home` bash script, though, some operations are done in different order due to the `cmd` script language limitations. If environment variable is set, the Python script `find_spark_home.py` will not be run. The process can fail if Python is not installed, but it will mostly use this way if PySpark is installed via `pip/conda`, thus, there is some Python in the system.
    
    ## How was this patch tested?
    
    Tested on local installation.
    
    Author: Jakub Nowacki <j.s.nowacki@gmail.com>
    
    Closes #19807 from jsnowacki/fix_spark_cmds_2.

[33mcommit ad57141f9499a4017c39fe336394c7084356df39[m
Author: Kazuaki Ishizaki <ishizaki@jp.ibm.com>
Date:   Fri Nov 24 12:08:49 2017 +0100

    [SPARK-22595][SQL] fix flaky test: CastSuite.SPARK-22500: cast for struct should not generate codes beyond 64KB
    
    This PR reduces the number of fields in the test case of `CastSuite` to fix an issue that is pointed at [here](https://github.com/apache/spark/pull/19800#issuecomment-346634950).
    
    ```
    java.lang.OutOfMemoryError: GC overhead limit exceeded
    java.lang.OutOfMemoryError: GC overhead limit exceeded
            at org.codehaus.janino.UnitCompiler.findClass(UnitCompiler.java:10971)
            at org.codehaus.janino.UnitCompiler.findTypeByName(UnitCompiler.java:7607)
            at org.codehaus.janino.UnitCompiler.getReferenceType(UnitCompiler.java:5758)
            at org.codehaus.janino.UnitCompiler.getType2(UnitCompiler.java:5732)
            at org.codehaus.janino.UnitCompiler.access$13200(UnitCompiler.java:206)
            at org.codehaus.janino.UnitCompiler$18.visitReferenceType(UnitCompiler.java:5668)
            at org.codehaus.janino.UnitCompiler$18.visitReferenceType(UnitCompiler.java:5660)
            at org.codehaus.janino.Java$ReferenceType.accept(Java.java:3356)
            at org.codehaus.janino.UnitCompiler.getType(UnitCompiler.java:5660)
            at org.codehaus.janino.UnitCompiler.buildLocalVariableMap(UnitCompiler.java:2892)
            at org.codehaus.janino.UnitCompiler.compile(UnitCompiler.java:2764)
            at org.codehaus.janino.UnitCompiler.compileDeclaredMethods(UnitCompiler.java:1262)
            at org.codehaus.janino.UnitCompiler.compileDeclaredMethods(UnitCompiler.java:1234)
            at org.codehaus.janino.UnitCompiler.compile2(UnitCompiler.java:538)
            at org.codehaus.janino.UnitCompiler.compile2(UnitCompiler.java:890)
            at org.codehaus.janino.UnitCompiler.compile2(UnitCompiler.java:894)
            at org.codehaus.janino.UnitCompiler.access$600(UnitCompiler.java:206)
            at org.codehaus.janino.UnitCompiler$2.visitMemberClassDeclaration(UnitCompiler.java:377)
            at org.codehaus.janino.UnitCompiler$2.visitMemberClassDeclaration(UnitCompiler.java:369)
            at org.codehaus.janino.Java$MemberClassDeclaration.accept(Java.java:1128)
            at org.codehaus.janino.UnitCompiler.compile(UnitCompiler.java:369)
            at org.codehaus.janino.UnitCompiler.compileDeclaredMemberTypes(UnitCompiler.java:1209)
            at org.codehaus.janino.UnitCompiler.compile2(UnitCompiler.java:564)
            at org.codehaus.janino.UnitCompiler.compile2(UnitCompiler.java:890)
            at org.codehaus.janino.UnitCompiler.compile2(UnitCompiler.java:894)
            at org.codehaus.janino.UnitCompiler.access$600(UnitCompiler.java:206)
            at org.codehaus.janino.UnitCompiler$2.visitMemberClassDeclaration(UnitCompiler.java:377)
            at org.codehaus.janino.UnitCompiler$2.visitMemberClassDeclaration(UnitCompiler.java:369)
            at org.codehaus.janino.Java$MemberClassDeclaration.accept(Java.java:1128)
            at org.codehaus.janino.UnitCompiler.compile(UnitCompiler.java:369)
            at org.codehaus.janino.UnitCompiler.compileDeclaredMemberTypes(UnitCompiler.java:1209)
            at org.codehaus.janino.UnitCompiler.compile2(UnitCompiler.java:564)
    ...
    ```
    
    Used existing test case
    
    Author: Kazuaki Ishizaki <ishizaki@jp.ibm.com>
    
    Closes #19806 from kiszk/SPARK-22595.
    
    (cherry picked from commit 554adc77d24c411a6df6d38c596aa33cdf68f3c1)
    Signed-off-by: Wenchen Fan <wenchen@databricks.com>

[33mcommit f4c457a308b0226aa0e7a1714c19046376e03409[m
Author: Liang-Chi Hsieh <viirya@gmail.com>
Date:   Fri Nov 24 11:46:58 2017 +0100

    [SPARK-22591][SQL] GenerateOrdering shouldn't change CodegenContext.INPUT_ROW
    
    ## What changes were proposed in this pull request?
    
    When I played with codegen in developing another PR, I found the value of `CodegenContext.INPUT_ROW` is not reliable. Under wholestage codegen, it is assigned to null first and then suddenly changed to `i`.
    
    The reason is `GenerateOrdering` changes `CodegenContext.INPUT_ROW` but doesn't restore it back.
    
    ## How was this patch tested?
    
    Added test.
    
    Author: Liang-Chi Hsieh <viirya@gmail.com>
    
    Closes #19800 from viirya/SPARK-22591.
    
    (cherry picked from commit 62a826f17c549ed93300bdce562db56bddd5d959)
    Signed-off-by: Wenchen Fan <wenchen@databricks.com>

[33mcommit f8e73d029d247d594793d832acd43041ac65b136[m
Author: vinodkc <vinod.kc.in@gmail.com>
Date:   Fri Nov 24 11:42:47 2017 +0100

    [SPARK-17920][SQL] [FOLLOWUP] Backport PR 19779 to branch-2.2
    
    ## What changes were proposed in this pull request?
    
    A followup of  https://github.com/apache/spark/pull/19795 , to simplify the file creation.
    
    ## How was this patch tested?
    
    Only test case is updated
    
    Author: vinodkc <vinod.kc.in@gmail.com>
    
    Closes #19809 from vinodkc/br_FollowupSPARK-17920_branch-2.2.

[33mcommit b17f4063cdf52c101ae562ac2a885918acd172ac[m
Author: vinodkc <vinod.kc.in@gmail.com>
Date:   Wed Nov 22 09:21:26 2017 -0800

    [SPARK-17920][SPARK-19580][SPARK-19878][SQL] Backport PR 19779 to branch-2.2 - Support writing to Hive table which uses Avro schema url 'avro.schema.url'
    
    ## What changes were proposed in this pull request?
    
    > Backport https://github.com/apa