# MYDB:简易实现手写数据库

MYDB 是一个 Java 实现的简单的数据库，来自[原作者代码](https://github.com/senshinya/MYDB)以及[原作者教程](https://shinya.click/projects/mydb/mydb1/)。

花了几天时间阅读了之后，部分存在比较难懂的地方，于是在仓库代码中结合了自己理解给原代码添加了许多便于理解的注释，可以理解成阅读笔记，并且将核心的逻辑关系总结成了两张图，如下所示。建议想学习该项目的javaer，结合这份添加了注释后的代码以及[原作者教程](https://shinya.click/projects/mydb/mydb1/)来相互理解，迷糊的时候再结合下面两张图片梳理梳理逻辑关系。

逻辑关系图1：
<p align="center">
  <img src="img/pic2New.png" alt="逻辑关系图1"/>
</p>
逻辑关系图2：
<p align="center">
  <img src="img/pic2.png" alt="逻辑关系图2"/>
</p>

过了10多天，忘了七七八八了，于是重新看了遍[原作者教程](https://shinya.click/projects/mydb/mydb1/)，并且做了几张图来汇总以便面经时快速复习：

缓存知识点：

<p align="center">
  <img src="img/review1.png" alt="缓存知识点"/>
</p>
页面知识点：


<p align="center">
  <img src="img/review2.png" alt="页面知识点"/>
</p>
日志知识点：


<p align="center">
  <img src="img/review3.png" alt="日志知识点"/>
</p>
如何解决冲突和隔离级别：


<p align="center">
  <img src="img/review4.png" alt="如何解决冲突和隔离级别"/>
</p>
版本跳跃问题的检查与死锁检测：


<p align="center">
  <img src="img/review5.png" alt="版本跳跃问题的检查与死锁检测"/>
</p>
解释SQL语句的例子：


<p align="center">
  <img src="img/review6.png" alt="解释SQL语句的例子"/>
</p>
