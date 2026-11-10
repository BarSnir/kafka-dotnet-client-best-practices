FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG APP
WORKDIR /src
COPY Directory.Packages.props confluent-cloud-dotnet-best-practices.sln ./
COPY src ./src
RUN dotnet restore && dotnet publish src/${APP}/${APP}.csproj -c Release -o /out /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /out ./
COPY schemas /app/schemas
ENTRYPOINT ["dotnet"]
CMD ["Producer.dll"]