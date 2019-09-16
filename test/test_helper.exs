ExUnit.start()

File.ls!("./test/support")
|> Enum.each(fn file -> Code.require_file("support/#{file}", __DIR__) end)
