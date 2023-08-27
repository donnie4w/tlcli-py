### client for tldb in python

------------

See the example at  http://tlnet.top/tlcli

             cli = client()
             cli.newConnect(True,"127.0.0.1", 7001, "mycli=123")
              1.newConnect 第一个参数是 是否 使用tls。如果服务器启动客户端安全链接协议，那么客户端应该将该参数设置为True。
              2.newConnect 第二个参数 是服务器启动客户端服务ip与端口
              3.newConnect 第三个参数 是访问的用户名密码，用等于号连接起来