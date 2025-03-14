const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const { TMP_DIR, outputDir } = require('./config');

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
 * Each segment has a start, end, and a flag indicating if it’s a blackout segment.
 * @param {number} totalDuration 
 * @param {Array} blackoutSegments 
 * @returns {Array}
 */
function buildSegments(totalDuration, blackoutSegments) {
  // Map each blackout segment to an object with start and end.
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
 * @param {string} inputPath 
 * @param {Array} blackoutSegments 
 * @returns {object} { normalPlaylistPath, blackoutPlaylistPath }
 */
function createM3U8WithExactSegments(inputPath, blackoutSegments) {
  // Get video properties.
  const totalDuration = getVideoDuration(inputPath);
  const resolution = getVideoResolution(inputPath);
  // Build segments.
  const segments = buildSegments(totalDuration, blackoutSegments);
  
  // Extract segments.
  segments.forEach((segment, index) => {
    // Always extract the original segment.
    extractSegment(inputPath, segment, index, outputDir);
    // For blackout segments, generate a blackout version.
    if (segment.isBlackout) {
      generateBlackoutSegment(segment, index, resolution, outputDir);
    }
  });
  
  // Generate playlists.
  const normalPlaylistContent = generatePlaylistContent(segments, 'normal');
  const blackoutPlaylistContent = generatePlaylistContent(segments, 'blackout');
  
  const normalPlaylistPath = path.join(outputDir, 'output.m3u8');
  fs.writeFileSync(normalPlaylistPath, normalPlaylistContent);
  const blackoutPlaylistPath = path.join(outputDir, 'blackout.m3u8');
  fs.writeFileSync(blackoutPlaylistPath, blackoutPlaylistContent);
  
  return { normalPlaylistPath, blackoutPlaylistPath };
}

/**
 * Updates a playlist file's content by replacing local segment filenames with their corresponding S3 URLs.
 */
function updatePlaylistContent(playlistPath, fileUrlMapping) {
  let content = fs.readFileSync(playlistPath, 'utf8');
  const lines = content.split('\n').map(line => {
    const trimmed = line.trim();
    if (trimmed.endsWith('.ts') && fileUrlMapping[trimmed]) {
      return fileUrlMapping[trimmed];
    }
    return line;
  });
  return lines.join('\n');
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
  TMP_DIR,
  outputDir
};
