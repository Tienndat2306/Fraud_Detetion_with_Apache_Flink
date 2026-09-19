@REM ----------------------------------------------------------------------------
@REM Licensed to the Apache Software Foundation (ASF) under one
@REM or more contributor license agreements.
@REM ----------------------------------------------------------------------------
@REM Maven start up batch script
@REM ----------------------------------------------------------------------------

@IF "%__MVNW_ARG0_USED%"=="" SET __MVNW_ARG0_USED=true& SET MVNW_ARGLINE=%* & GOTO :start_mvnw
@GOTO :eof
:start_mvnw

@REM Find project base directory
@SET MAVEN_PROJECTBASEDIR=%~dp0
@IF NOT "%MAVEN_PROJECTBASEDIR%"=="" GOTO endDetectBaseDir
@SET MAVEN_PROJECTBASEDIR=%CD%
:endDetectBaseDir
@IF "%MAVEN_PROJECTBASEDIR:~-1%"=="\" SET MAVEN_PROJECTBASEDIR=%MAVEN_PROJECTBASEDIR:~0,-1%

@SET WRAPPER_JAR="%MAVEN_PROJECTBASEDIR%\.mvn\wrapper\maven-wrapper.jar"
@SET WRAPPER_PROPERTIES="%MAVEN_PROJECTBASEDIR%\.mvn\wrapper\maven-wrapper.properties"

@REM Extension to allow automatically downloading the maven-wrapper.jar from Maven-central
@SET WRAPPER_URL="https://repo.maven.apache.org/maven2/org/apache/maven/wrapper/maven-wrapper/3.2.0/maven-wrapper-3.2.0.jar"

@IF EXIST %WRAPPER_JAR% (
    @ECHO Found %WRAPPER_JAR%
) ELSE (
    @ECHO Downloading from: %WRAPPER_URL%
    powershell -Command "&{"^
		"$webclient = new-object System.Net.WebClient;"^
		"$webclient.DownloadFile('%WRAPPER_URL%', '%WRAPPER_JAR%');"^
		"}"
    @ECHO Downloaded %WRAPPER_JAR%
)

@REM Run Maven
"%JAVA_HOME%\bin\java.exe" ^
  "-Dmaven.multiModuleProjectDirectory=%MAVEN_PROJECTBASEDIR%" ^
  %MAVEN_OPTS% ^
  -classpath %WRAPPER_JAR% ^
  org.apache.maven.wrapper.MavenWrapperMain ^
  %MVNW_ARGLINE%
if ERRORLEVEL 1 goto error
goto end

:error
set ERROR_CODE=1

:end
@endlocal & set ERROR_CODE=%ERROR_CODE%

cmd /C exit /B %ERROR_CODE%
