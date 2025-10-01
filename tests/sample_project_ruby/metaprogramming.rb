class DynamicGreeter
  [:hello, :goodbye].each do |method|
    define_method(method) do |name|
      "#{method.capitalize}, #{name}!"
    end
  end

  def method_missing(name, *args)
    "Method #{name} not defined"
  end
end

dg = DynamicGreeter.new
puts dg.hello("Pablo")
puts dg.foobar("X")
