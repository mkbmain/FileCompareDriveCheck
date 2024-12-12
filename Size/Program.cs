using System.Security.Cryptography;
using System.Text.Json.Serialization;
using Microsoft.Data.Sqlite;
using Mkb.DapperRepo.Repo;

namespace Size;

class Program
{
    private const string Path = "/home/mkb/output.json";
    public const string RootDrive = "/media/mkb/80ea538c-3752-46c3-9421-3bd24fce14af/";

    static async Task Main(string[] args)
    {
        var repo = new SqlRepoAsync(() => new SqliteConnection("Data Source=/home/mkb/Harddrive.sqlite"));
        await PopulateDb(repo);
        await Update(repo);
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

            file.Hash = FileNode.CalculateMd5(fullPath);
            await repoAsync.Update(file);
        }
    }

    static async Task PopulateDb(SqlRepoAsync repo)
    {
        const int chunkSize = 250;
        var rootFolderNode = new FolderNode(RootDrive);

        var allFileNodes = rootFolderNode.GetAllFilesAndChildrenFiles().ToArray();

        var allDbRecords = await repo.GetAll<DbFile>();
        var dbLookup = allDbRecords.GroupBy(w => w.FilePath).ToDictionary(q => q.Key, q => q.First());
        var toInsert = new List<FileNode>();
        var fileNodeLookup = allFileNodes.Select(w => w.RelativePath).ToHashSet();
        foreach (var fileNode in allFileNodes)
        {
            if (!dbLookup.TryGetValue(fileNode.RelativePath, out var file))
            {
                toInsert.Add(fileNode);
                continue;
            }

            if (file.Size == fileNode.Size) continue;
            file.Hash = null;
            file.Size = fileNode.Size;
            file.PrettySize = fileNode.PrettySize;
            await repo.Update(file);
        }

        foreach (var item in dbLookup.Where(item => !fileNodeLookup.Contains(item.Key)).Chunk(chunkSize))
        {
            var sql = $"Delete from files where id in ({string.Join(",", item.Select(q => q.Value.Id))})";
            await repo.Execute(sql);
        }

        foreach (var batch in toInsert.Chunk(chunkSize))
        {
            const string rawInsert = "insert into Files(FileName, FilePath, Hash, PrettySize,Size)\nvalues \n";
            var sql = rawInsert + string.Join(",",
                batch.Select(q =>
                    $"('{q.Name.Replace("'", "''")}','{q.RelativePath.Replace("'", "''")}',null,'{q.PrettySize}',{q.Size})"));
            await repo.Execute(sql);
        }
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

class FolderNode : Node
{
    public FolderNode[] Children { get; private set; }

    public FileNode[] Files { get; private set; }

    public IEnumerable<FileNode> GetAllFilesAndChildrenFiles() => Children
        .SelectMany(c => c.GetAllFilesAndChildrenFiles()).Concat(Files)
        .OrderByDescending(q => q.Size);

    public FolderNode(string path)
    {
        var file = new DirectoryInfo(path);
        RelativePath = file.FullName;
        Name = file.Name;
        Children = Directory.GetDirectories(path, "*", SearchOption.TopDirectoryOnly)
            .Select(q => new FolderNode(q)).OrderByDescending(q => q.Size).ToArray();
        Files = Directory.GetFiles(path).Select(q => new FileNode(q)).OrderByDescending(q => q.Size).ToArray();
        Size = Files.Sum(q => q.Size) + Children.Sum(q => q.Size);
    }
}

public class FileNode : Node
{
    public string? Md5 { get; set; } = null;

    public FileNode(string path)
    {
        var file = new FileInfo(path);
        RelativePath = file.FullName.Replace(Program.RootDrive, "");
        Name = file.Name;
        Size = file.Length;
    }

    public static string CalculateMd5(string filename)
    {
        using var md5 = MD5.Create();
        using var stream = File.OpenRead(filename);
        var hash = md5.ComputeHash(stream);
        return BitConverter.ToString(hash).Replace("-", "").ToLowerInvariant();
    }
}

public abstract class Node
{
    public string Name { get; protected set; }

    public string RelativePath { get; protected set; }

    public long Size { get; protected set; }

    public string PrettySize => BytesToString(Size);

    private static readonly string[] Suffix = { "", "K", "M", "G", "T", "P", "E" }; //Longs run out around EB

    public static string BytesToString(long byteCount)
    {
        if (byteCount == 0) return $"0{Suffix[0]}";
        var bytes = Math.Abs(byteCount);
        var place = Convert.ToInt32(Math.Floor(Math.Log(bytes, 1024)));
        var num = Math.Round(bytes / Math.Pow(1024, place), 1);
        return $"{(Math.Sign(byteCount) * num)}{Suffix[place]}B";
    }
}