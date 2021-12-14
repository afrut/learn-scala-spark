# Clear console.
Clear-Host

# Expected firstparameter is project name.
if($args.Length -gt 0) {
  $projName = $args[0]

  # Create directories.
  $projDir = New-Item -Path . -Name $projName -ItemType "directory" -Force
  $projPath = $projDir.FullName
  New-Item -ItemType "directory" -Force -Path $projPath -Name "target" | Out-Null
  New-Item -ItemType "directory" -Force -Path $projPath -Name "project" | Out-Null
  New-Item -ItemType "directory" -Force -Path $projPath -Name "src" | Out-Null
  New-Item -ItemType "directory" -Force -Path "$projPath\src\" -Name "main" | Out-Null
  New-Item -ItemType "directory" -Force -Path "$projPath\src\main\" -Name "java" | Out-Null
  New-Item -ItemType "directory" -Force -Path "$projPath\src\main\" -Name "resources" | Out-Null
  New-Item -ItemType "directory" -Force -Path "$projPath\src\main\" -Name "scala" | Out-Null
  New-Item -ItemType "directory" -Force -Path "$projPath\src\" -Name "test" | Out-Null
  New-Item -ItemType "directory" -Force -Path "$projPath\src\test\" -Name "java" | Out-Null
  New-Item -ItemType "directory" -Force -Path "$projPath\src\test\" -Name "resources" | Out-Null
  New-Item -ItemType "directory" -Force -Path "$projPath\src\test\" -Name "scala" | Out-Null

  # Output created directories.
  Write-Host "Created directories"
  Get-ChildItem -Recurse $projPath | Sort-Object FullName | ForEach-Object {
    Write-Host "  $(Resolve-Path -Path $_.FullName -Relative)"
  }

  $buildFile = New-Item -Path $projPath -Name "build.sbt" -ItemType "file" -Force
  "scalaVersion := `"2.13.3`"" | Out-File -Encoding ASCII -File $buildFile -Append
  "name := `"$projName`"" | Out-File -Encoding ASCII -File $buildFile -Append
  "version := `"1.0`"" | Out-File -Encoding ASCII -File $buildFile -Append

  $mainFile = New-Item -path "$projPath\src\main\scala\" -Name "Main.scala" -ItemType "file" -Force
  "object Main extends App {" | Out-File -Encoding ASCII -File $mainFile -Append
  "  println(`"Hello, World!`")" | Out-File -Encoding ASCII -File $mainFile -Append
  "}" | Out-File -Encoding ASCII -File $mainFile -Append

  $ignoreFile = New-Item -path "$projPath" -Name ".gitignore" -ItemType "file" -Force
  "target" | Out-File -Encoding ASCII -File $ignoreFile -Append
}
else {
  Write-Host "No project name passed."
}