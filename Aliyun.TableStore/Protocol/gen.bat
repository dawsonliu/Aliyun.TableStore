SET TP=%USERPROFILE%\.nuget\packages\google.protobuf.tools\3.5.1\tools\windows_x64

%TP%\protoc.exe -I=. --csharp_out=. ots_protocol.3.proto

@pause