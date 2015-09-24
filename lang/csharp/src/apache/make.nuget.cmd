cd main
nuget pack -Build -Symbols -Properties Configuration=Release

cd ..\codegen
nuget pack -Build -Symbols -Properties Configuration=Release

cd ..\msbuild
nuget pack -Build -Symbols -Properties Configuration=Release
