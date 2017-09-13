Pod::Spec.new do |s|
  s.name         = "SQLPackRat"
  s.version      = "0.5.1"
  s.summary      = "Simple Objective-C wrapper for SQLite."
  s.description  = <<-DESC
                   SQLPackRat is a simple Objective-C wrapper for SQLite.
                   DESC
  s.homepage     = "https://github.com/tewha/SQLPackRat"
  s.license      = { :type => "MIT", :file => "LICENSE" }
  s.author             = { "Steven Fisher" => "tewha@me.com" }
  s.social_media_url   = "http://twitter.com/tewha"
  s.ios.deployment_target = "8.0"
  s.osx.deployment_target = "10.10"
  s.source       = { :git => "https://github.com/tewha/SQLPackRat.git", :tag => "#{s.version}" }
  s.source_files  = "SQLPackRat", "Categories"
  s.exclude_files = "Classes/Exclude"
  s.libraries = "sqlite3"
end
