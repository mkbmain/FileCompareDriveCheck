using System.Security.Cryptography;
using System.Text.Json.Serialization;
using Microsoft.Data.Sqlite;
using Mkb.DapperRepo.Repo;

namespace Size;

class Program
{
    private const string Path = "/home/mkb/output.json";
    public const string RootDrive = "/media/mkb/80ea538c-3752-46c3-9421-3bd24fce14af/";

    private const string SqlToCreateTable = @"create table if not exists Files
(
    Id         integer
        constraint Files_pk
            primary key autoincrement,
    FileName   TEXT,
    FilePath   TEXT,
    Hash       text,
    PrettySize text,
    Size       INT
);";

    static async Task Main(string[] args)
    {
        var repo = new SqlRepoAsync(() => new SqliteConnection("Data Source=/home/mkb/Harddrive2.sqlite"));
        await repo.Execute(SqlToCreateTable);
        await PopulateDb(repo);
        await Update(repo);
    }

    private static readonly string[] Suffix = { "", "K", "M", "G", "T", "P", "E" }; //Longs run out around EB

    private static string BytesToString(long byteCount)
    {
        if (byteCount == 0) return $"0{Suffix[0]}";
        var bytes = Math.Abs(byteCount);
        var place = Convert.ToInt32(Math.Floor(Math.Log(bytes, 1024)));
        var num = Math.Round(bytes / Math.Pow(1024, place), 1);
        return $"{(Math.Sign(byteCount) * num)}{Suffix[place]}B";
    }

    private static string CalculateMd5(string filename)
    {
        using var md5 = MD5.Create();
        using var stream = File.OpenRead(filename);
        var hash = md5.ComputeHash(stream);
        return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
    }


    static async Task Update(SqlRepoAsync repoAsync)
    {
        var files = await repoAsync.GetAll<DbFile>();

        foreach (var file in files.Where(w => w.Hash is null))
        {
            var fullPath = System.IO.Path.Combine(RootDrive, file.FilePath);
            if (!File.Exists(fullPath))
            {
                await repoAsync.Delete(file);
                continue;
            }

            file.Hash = CalculateMd5(fullPath);
            await repoAsync.Update(file);
        }
    }

    static async Task PopulateDb(SqlRepoAsync repo)
    {
        const int chunkSize = 250;

        var allFileNodes = Directory.GetFiles(RootDrive, "*.*", SearchOption.AllDirectories)
            .Select(w => new FileInfo(w))
            .Select(w => new
            {
                RelativePath = w.FullName.Replace(Program.RootDrive, ""),
                FileInfo = w
            }).ToArray();

        var allDbRecords = await repo.GetAll<DbFile>();
        var dbLookup = allDbRecords.GroupBy(w => w.FilePath).ToDictionary(q => q.Key, q => q.First());
        var toInsert = new List<string>();
        var fileNodeLookup = allFileNodes.Select(w => w.RelativePath).ToHashSet();
        foreach (var fileNode in allFileNodes)
        {
            if (!dbLookup.TryGetValue(fileNode.RelativePath, out var file))
            {
                toInsert.Add(
                    $"('{fileNode.FileInfo.Name.Replace("'", "''")}','{fileNode.RelativePath.Replace("'", "''")}',null,'{BytesToString(fileNode.FileInfo.Length)}',{fileNode.FileInfo.Length})");
                if (toInsert.Count > chunkSize)
                {
                    await Insert(toInsert, repo);
                    toInsert = new List<string>();
                }

                continue;
            }

            if (file.Size == fileNode.FileInfo.Length) continue;
            file.Hash = null;
            file.Size = fileNode.FileInfo.Length;
            file.PrettySize = BytesToString(fileNode.FileInfo.Length);
            await repo.Update(file);
        }

        await Insert(toInsert, repo);
        
        foreach (var item in dbLookup.Where(item => !fileNodeLookup.Contains(item.Key)).Chunk(chunkSize))
        {
            var sql = $"Delete from files where id in ({string.Join(",", item.Select(q => q.Value.Id))})";
            await repo.Execute(sql);
        }
    }

    static async Task Insert(IEnumerable<string> lines, SqlRepoAsync repo)
    {
        if(!lines.Any()) return;
        const string rawInsert = "insert into Files(FileName, FilePath, Hash, PrettySize,Size)\nvalues \n";
        var sql = rawInsert + string.Join(",", lines);
        await repo.Execute(sql);
    }
}

[Mkb.DapperRepo.Attributes.SqlTableName("Files")]
class DbFile
{
    [Mkb.DapperRepo.Attributes.PrimaryKey] public int Id { get; set; }
    public string Hash { get; set; }
    public long Size { get; set; }
    public string FileName { get; set; }
    public string PrettySize { get; set; }
    public string FilePath { get; set; }
}