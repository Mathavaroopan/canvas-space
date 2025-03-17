const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const { GetObjectCommand, ListObjectsV2Command } = require('@aws-sdk/client-s3');
// Use the standard stream module and promisify its pipeline for broader compatibility.
const { pipeline } = require('stream');
const { promisify } = require('util');
const streamPipeline = promisify(pipeline);
const { TMP_DIR, outputDir } = require('./config');
const { downloadFileFromS3 } = require('./s3Processing');

/**
 * Gets the duration (in seconds) of a video using ffprobe.
 * @param {string} inputPath 
 * @returns {number}
 */
function getVideoDuration(inputPath) {
  const durationOutput = execSync(
    `ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "${inputPath}"`
  ).toString().trim();
  return parseFloat(durationOutput);
}

/**
 * Gets the resolution of a video using ffprobe.
 * @param {string} inputPath 
 * @returns {string} e.g. "1280x720"
 */
function getVideoResolution(inputPath) {
  return execSync(
    `ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of csv=s=x:p=0 "${inputPath}"`
  ).toString().trim();
}

/**
 * Given total video duration and an array of blackout lock objects,
 * builds an array of segment objects.
 * Each segment has a start, end, and a flag indicating if it's a blackout segment.
 * @param {number} totalDuration 
 * @param {Array} blackoutSegments 
 * @returns {Array}
 */
function buildSegments(totalDuration, blackoutSegments) {
  const customSegments = blackoutSegments.map(seg => ({
    start: Number(seg.startTime),
    end: Number(seg.endTime)
  }));
  customSegments.sort((a, b) => a.start - b.start);
  const segments = [];
  let currentTime = 0;
  for (const seg of customSegments) {
    if (seg.start > currentTime) {
      segments.push({ start: currentTime, end: seg.start, isBlackout: false });
    }
    segments.push({ start: seg.start, end: seg.end, isBlackout: true });
    currentTime = seg.end;
  }
  if (currentTime < totalDuration) {
    segments.push({ start: currentTime, end: totalDuration, isBlackout: false });
  }
  return segments;
}

/**
 * Extracts a video segment (non-blackout) as a .ts file.
 * @param {string} inputPath 
 * @param {object} segment 
 * @param {number} index 
 * @param {string} outputDir 
 * @returns {string} Path to extracted segment file.
 */
function extractSegment(inputPath, segment, index, outputDir) {
  const segmentPath = path.join(outputDir, `segment_${String(index).padStart(3, '0')}.ts`);
  execSync(
    `ffmpeg -y -i "${inputPath}" -ss ${segment.start} -to ${segment.end} -c:v libx264 -c:a aac -f mpegts "${segmentPath}"`
  );
  return segmentPath;
}

/**
 * Generates a blackout segment as a .ts file.
 * @param {object} segment 
 * @param {number} index 
 * @param {string} resolution 
 * @param {string} outputDir 
 * @returns {string} Path to generated blackout file.
 */
function generateBlackoutSegment(segment, index, resolution, outputDir) {
  const blackoutPath = path.join(outputDir, `blackout_${String(index).padStart(3, '0')}.ts`);
  const segDuration = segment.end - segment.start;
  execSync(
    `ffmpeg -y -f lavfi -i color=c=black:s=${resolution}:r=30 -f lavfi -i anullsrc=channel_layout=stereo:sample_rate=48000 -t ${segDuration} -c:v libx264 -pix_fmt yuv420p -c:a aac -shortest -f mpegts "${blackoutPath}"`
  );
  return blackoutPath;
}

/**
 * Generates an HLS playlist file content.
 * @param {Array} segments 
 * @param {string} type "normal" or "blackout"
 * @returns {string} Playlist content.
 */
function generatePlaylistContent(segments, type) {
  const maxDuration = Math.ceil(Math.max(...segments.map(s => s.end - s.start)));
  const lines = [
    '#EXTM3U',
    '#EXT-X-VERSION:3',
    `#EXT-X-TARGETDURATION:${maxDuration}`,
    '#EXT-X-MEDIA-SEQUENCE:0',
    '#EXT-X-PLAYLIST-TYPE:VOD'
  ];
  segments.forEach((segment, index) => {
    const segDuration = segment.end - segment.start;
    lines.push(`#EXTINF:${segDuration.toFixed(6)},`);
    if (type === 'normal') {
      lines.push(`segment_${String(index).padStart(3, '0')}.ts`);
    } else if (type === 'blackout') {
      lines.push(segment.isBlackout
        ? `blackout_${String(index).padStart(3, '0')}.ts`
        : `segment_${String(index).padStart(3, '0')}.ts`);
    }
  });
  lines.push('#EXT-X-ENDLIST');
  return lines.join('\n');
}

/**
 * Creates two HLS playlists (normal and blackout) by processing the video.
 * Accepts additional parameters for custom file names.
 * @param {string} inputPath 
 * @param {Array} blackoutSegments 
 * @param {string} normalFileName - Desired file name for the normal playlist (e.g., "new-name.m3u8")
 * @param {string} blackoutFileName - Desired file name for the blackout playlist (e.g., "new-blackout.m3u8")
 * @returns {object} { normalPlaylistPath, blackoutPlaylistPath }
 */
function createM3U8WithExactSegments(inputPath, blackoutSegments, normalFileName, blackoutFileName) {
  const totalDuration = getVideoDuration(inputPath);
  const resolution = getVideoResolution(inputPath);
  const segments = buildSegments(totalDuration, blackoutSegments);
  
  segments.forEach((segment, index) => {
    extractSegment(inputPath, segment, index, outputDir);
    if (segment.isBlackout) {
      generateBlackoutSegment(segment, index, resolution, outputDir);
    }
  });
  
  const normalPlaylistContent = generatePlaylistContent(segments, 'normal');
  const blackoutPlaylistContent = generatePlaylistContent(segments, 'blackout');
  
  const normalPlaylistPath = path.join(outputDir, normalFileName);
  fs.writeFileSync(normalPlaylistPath, normalPlaylistContent);
  const blackoutPlaylistPath = path.join(outputDir, blackoutFileName);
  fs.writeFileSync(blackoutPlaylistPath, blackoutPlaylistContent);
  
  return { normalPlaylistPath, blackoutPlaylistPath };
}

/**
 * Updates a playlist file's content by replacing local segment filenames with their corresponding S3 URLs.
 * @param {string} playlistPath 
 * @param {object} fileUrlMapping 
 * @returns {string} Updated playlist content.
 */
function updatePlaylistContent(playlistPath, fileUrlMapping) {
  const content = fs.readFileSync(playlistPath, 'utf8');
  const lines = content.split('\n').map(line => {
    const trimmed = line.trim();
    if (trimmed.endsWith('.ts') && fileUrlMapping[trimmed]) {
      return fileUrlMapping[trimmed];
    }
    return line;
  });
  return lines.join('\n');
}

/**
 * Downloads all objects in the given S3 folder (prefix) into a local folder.
 * Returns the path to the local folder.
 */
async function downloadM3U8Folder(s3Client, bucketName, folderKey) {
  const localFolder = path.join(TMP_DIR, `m3u8_download_${Date.now()}`);
  if (!fs.existsSync(localFolder)) {
    fs.mkdirSync(localFolder, { recursive: true });
  }
  const listParams = { Bucket: bucketName, Prefix: folderKey };
  const listCommand = new ListObjectsV2Command(listParams);
  const listData = await s3Client.send(listCommand);
  if (!listData.Contents || listData.Contents.length === 0) {
    throw new Error("No files found in the provided folder key.");
  }
  for (const obj of listData.Contents) {
    if (obj.Key.endsWith('/')) continue;
    const filename = path.basename(obj.Key);
    const localFilePath = path.join(localFolder, filename);
    console.log("Downloading:", obj.Key);
    const getObjectParams = { Bucket: bucketName, Key: obj.Key };
    const getObjectCommand = new GetObjectCommand(getObjectParams);
    const fileResponse = await s3Client.send(getObjectCommand);
    await streamPipeline(fileResponse.Body, fs.createWriteStream(localFilePath));
    console.log(`Downloaded: ${filename}`);
  }
  console.log("Download m3u8 is finished");
  
  return localFolder;
}

/**
 * Reads and sanitizes a local m3u8 file so that TS segment lines contain only the filename.
 */
function sanitizeLocalM3U8(m3u8Path) {
  const content = fs.readFileSync(m3u8Path, 'utf8');
  const sanitized = content.split('\n').map(line => {
    if (line.trim().endsWith('.ts')) {
      const parts = line.trim().split('/');
      return parts[parts.length - 1];
    }
    return line;
  }).join('\n');
  fs.writeFileSync(m3u8Path, sanitized);
}

/**
 * Processes the S3 original key and downloads the file/folder to a local MP4 path.
 * Handles different input types: m3u8 file, folder containing m3u8, or mp4 file.
 */
async function processSourceToLocalMp4(s3Client, bucketName, awsOriginalKey) {
  let localMp4Path;
  const ext = path.extname(awsOriginalKey).toLowerCase();

  if (ext === '.m3u8') {
    const folderKey = path.dirname(awsOriginalKey) + '/';
    console.log("downloading m3u8");
    const localFolder = await downloadM3U8Folder(s3Client, bucketName, folderKey);
    console.log("download m3u8 is finished");
    const m3u8Filename = path.basename(awsOriginalKey);
    const localM3u8Path = path.join(localFolder, m3u8Filename);
    console.log("sanitization starts");
    sanitizeLocalM3U8(localM3u8Path);
    console.log("sanitization done");
    localMp4Path = path.join(TMP_DIR, `${Date.now()}-converted.mp4`);
    execSync(`ffmpeg -protocol_whitelist "file,http,https,tcp,tls" -i "${localM3u8Path}" -c copy "${localMp4Path}"`);
    fs.rmSync(localFolder, { recursive: true, force: true });
    console.log("mp4 created");
  } else if (!ext) {
    let folderKey = awsOriginalKey;
    if (!folderKey.endsWith('/')) folderKey += '/';
    const localFolder = await downloadM3U8Folder(s3Client, bucketName, folderKey);
    const files = fs.readdirSync(localFolder);
    const m3u8File = files.find(file => file.endsWith('.m3u8'));
    if (!m3u8File) {
      throw new Error("No m3u8 file found in the provided folder.");
    }
    const localM3u8Path = path.join(localFolder, m3u8File);
    sanitizeLocalM3U8(localM3u8Path);
    localMp4Path = path.join(TMP_DIR, `${Date.now()}-converted.mp4`);
    execSync(`ffmpeg -protocol_whitelist "file,http,https,tcp,tls" -i "${localM3u8Path}" -c copy "${localMp4Path}"`);
    fs.rmSync(localFolder, { recursive: true, force: true });
  } else {
    localMp4Path = path.join(TMP_DIR, `${Date.now()}-original.mp4`);
    await downloadFileFromS3(s3Client, bucketName, awsOriginalKey, localMp4Path);
  }

  try {
    execSync(`ffprobe -v error -select_streams v:0 -show_entries stream=codec_name -of default=noprint_wrappers=1:nokey=1 "${localMp4Path}"`);
  } catch (error) {
    throw new Error(`Invalid or corrupted video file: ${error.message}`);
  }

  return localMp4Path;
}

module.exports = {
  getVideoDuration,
  getVideoResolution,
  buildSegments,
  extractSegment,
  generateBlackoutSegment,
  generatePlaylistContent,
  createM3U8WithExactSegments,
  updatePlaylistContent,
  downloadM3U8Folder,
  sanitizeLocalM3U8,
  processSourceToLocalMp4,
  TMP_DIR,
  outputDir
};
