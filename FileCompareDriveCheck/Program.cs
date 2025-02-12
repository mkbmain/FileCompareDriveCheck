using System.Security.Cryptography;
using Microsoft.Data.Sqlite;
using Mkb.DapperRepo.Repo;
using Mkb.DapperRepo.Search;

namespace FileCompareDriveCheck;

class Program
{
    private const string NameOfDbFiles = "Harddrive.sqlite";

    private const string SqlToCreateTable = """
                                            create table if not exists Files 
                                            (Id  integer constraint Files_pk primary key autoincrement,
                                             FileName   TEXT, FilePath   TEXT,  Hash       text,  PrettySize text,  Size       INT);
                                            """;

    private const string CommandText = """
                                       Options
                                       Build {PathToBuild}
                                           eg. Build /media/mkb/8tbSamsung/

                                       Compare {PathOne} {PathTwo}
                                           eg. Compare /media/mkb/8tbSamsung/ /media/mkb/LinuxSSD/
                                       """;

    private static async Task Compare(IEnumerable<string> args)
    {
        var pathOne = args.First();
        if (!Directory.Exists(pathOne))
        {
            Console.WriteLine($"Path {pathOne} does not exist.");
            return;
        }

        var pathTwo = args.Skip(1).First();
        if (!Directory.Exists(pathTwo))
        {
            Console.WriteLine($"Path {pathTwo} does not exist.");
            return;
        }

        var oldRepo = new SqlRepoAsync(() =>
            new SqliteConnection($"Data Source={Path.Combine(pathOne, NameOfDbFiles)}"));
        var newRepo =
            new SqlRepoAsync(() => new SqliteConnection($"Data Source={Path.Combine(pathTwo, NameOfDbFiles)}"));
        var allOldRecords = await oldRepo.GetAll<DbFile>();
        var allNewRecords = await newRepo.GetAll<DbFile>();

        var oldLookUp = allOldRecords.GroupBy(w => w.FilePath).ToDictionary(g => g.Key, g => g.ToList());
        var newLookUp = allNewRecords.GroupBy(w => w.FilePath).ToDictionary(g => g.Key, g => g.ToList());

        var itemsMissing = oldLookUp.Where(w => !newLookUp.ContainsKey(w.Key)).Select(w => w.Key).ToArray();
        var itemsMissingOld = newLookUp.Where(w => !oldLookUp.ContainsKey(w.Key)).Select(q => q.Key).ToArray();

        var union = string.Join(Environment.NewLine, itemsMissing.Union(itemsMissingOld));
        Console.WriteLine($"Missing{Environment.NewLine}{union}");
        foreach (var item in newLookUp)
        {
            if (!oldLookUp.TryGetValue(item.Key, out var oldItem)) continue;

            var hash = oldItem.First().Hash;
            var thisHash = item.Value.First().Hash;
            if (hash == thisHash) continue;
            Console.WriteLine($"File {item.Key}  -- Hash: {thisHash}, Hash: {hash} difference ");
        }

        Console.WriteLine("Done");
    }

    private static async Task Main(string[] args)
    {
        switch (args.FirstOrDefault()?.ToLower())
        {
            default:
                Console.WriteLine(CommandText);
                break;
            case "build":
                await BuildDb(args.Skip(1));
                return;
            case "compare":
                await Compare(args.Skip(1));
                return;
        }
    }


    private static async Task BuildDb(IEnumerable<string> args)
    {
        var rootDrive = string.Join(" ", args);
        if (!Directory.Exists(rootDrive))
        {
            Console.WriteLine("Location does not exist.");
            return;
        }

        var path = Path.Combine(rootDrive, NameOfDbFiles);
        var lite = new SqliteConnection($"Data Source={path}");
        var repo = new SqlRepoAsync(() => lite);
        await repo.Execute(SqlToCreateTable);
        await PopulateDb(repo, rootDrive);
        await Update(repo, rootDrive);
    }

    private static readonly string[] Suffix = ["", "K", "M", "G", "T", "P", "E"]; //Longs run out around EB

    private static string BytesToString(long byteCount)
    {
        if (byteCount == 0) return $"0{Suffix[0]}";
        var bytes = Math.Abs(byteCount);
        var place = Convert.ToInt32(Math.Floor(Math.Log(bytes, 1024)));
        var num = Math.Round(bytes / Math.Pow(1024, place), 1);
        return $"{(Math.Sign(byteCount) * num)}{Suffix[place]}B";
    }

    private static string? CalculateMd5(string filename)
    {
        using var md5 = MD5.Create();
        using var stream = File.OpenRead(filename);
        var hash = md5.ComputeHash(stream);
        return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
    }


    private static async Task Update(SqlRepoAsync repoAsync, string rootDrive)
    {
        Console.WriteLine("Update");
        var files =
            (await repoAsync.Search(new DbFile(), SearchCriteria.Create(nameof(DbFile.Hash), SearchType.IsNull)))
            .ToArray();
        var date = DateTime.Now;
        long totalSizeDone = 0;
        int countInBatch = 0;
        for (var i = 0; i < files.Length; i++)
        {
            var file = files[i];
            var fullPath = Path.Combine(rootDrive, file.FilePath);
            totalSizeDone += file.Size;
            if (!File.Exists(fullPath))
            {
                await repoAsync.Delete(file);
                continue;
            }

            if ((DateTime.Now - date).TotalSeconds > 60)
            {
                date = DateTime.Now;
                Console.WriteLine(
                    $"{date:t} Batch{i - countInBatch}, {i} / {files.Length}, Left {files.Length - i}, total done {BytesToString(totalSizeDone)}");
                countInBatch = i;
            }

            file.Hash = CalculateMd5(fullPath);
            await repoAsync.Update(file);
        }
    }

    private static async Task PopulateDb(SqlRepoAsync repo, string rootDrive)
    {
        const int chunkSize = 250;

        var allFileNodes = Directory.GetFiles(rootDrive, "*.*", SearchOption.AllDirectories)
            .Select(w => new FileInfo(w))
            .Select(w => new
            {
                RelativePath = w.FullName.Replace(rootDrive, ""),
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
                if (!File.Exists(fileNode.FileInfo.FullName)) continue;
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

    private static async Task Insert(List<string> lines, SqlRepoAsync repo)
    {
        if (lines.Count < 1) return;
        var sql = $"insert into Files(FileName, FilePath, Hash, PrettySize,Size) values {string.Join(",", lines)}";
        await repo.Execute(sql);
    }
}

[Mkb.DapperRepo.Attributes.SqlTableName("Files")]
class DbFile
{
    [Mkb.DapperRepo.Attributes.PrimaryKey] public int? Id { get; set; }

    public string? Hash { get; set; }
    public long Size { get; set; }
    public string FileName { get; set; }
    public string PrettySize { get; set; }
    public string FilePath { get; set; }
}