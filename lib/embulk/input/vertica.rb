Embulk::JavaPlugin.register_input(
  :vertica, "org.embulk.input.VerticaInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
